from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union, no_type_check

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import (
    Sequence as CogniteSequence,
)
from cognite.client.data_classes import (
    SequenceWrite as CogniteSequenceWrite,
)
from pydantic import Field, field_validator, model_validator

from wind_turbine.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelList,
    DomainModelWrite,
    DomainModelWriteList,
    DomainRelation,
    DomainRelationWrite,
    FloatFilter,
    GraphQLCore,
    NodeQueryCore,
    QueryCore,
    ResourcesWrite,
    SequenceGraphQL,
    SequenceRead,
    SequenceWrite,
    StringFilter,
    T_DomainModelList,
    as_direct_relation_reference,
    as_instance_dict_id,
    as_node_id,
    as_pygen_node_id,
    is_tuple_id,
)
from wind_turbine.data_classes._generating_unit import GeneratingUnit, GeneratingUnitWrite

if TYPE_CHECKING:
    from wind_turbine.data_classes._blade import Blade, BladeGraphQL, BladeList, BladeWrite, BladeWriteList
    from wind_turbine.data_classes._data_sheet import (
        DataSheet,
        DataSheetGraphQL,
        DataSheetList,
        DataSheetWrite,
        DataSheetWriteList,
    )
    from wind_turbine.data_classes._distance import (
        Distance,
        DistanceGraphQL,
        DistanceList,
        DistanceWrite,
        DistanceWriteList,
    )
    from wind_turbine.data_classes._nacelle import Nacelle, NacelleGraphQL, NacelleList, NacelleWrite, NacelleWriteList
    from wind_turbine.data_classes._rotor import Rotor, RotorGraphQL, RotorList, RotorWrite, RotorWriteList


__all__ = [
    "WindTurbine",
    "WindTurbineWrite",
    "WindTurbineApply",
    "WindTurbineList",
    "WindTurbineWriteList",
    "WindTurbineApplyList",
    "WindTurbineFields",
    "WindTurbineTextFields",
    "WindTurbineGraphQL",
]


WindTurbineTextFields = Literal["external_id", "description", "name", "power_curve", "windfarm"]
WindTurbineFields = Literal["external_id", "capacity", "description", "name", "power_curve", "windfarm"]

_WINDTURBINE_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "capacity": "capacity",
    "description": "description",
    "name": "name",
    "power_curve": "powerCurve",
    "windfarm": "windfarm",
}


class WindTurbineGraphQL(GraphQLCore):
    """This represents the reading version of wind turbine, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the wind turbine.
        data_record: The data record of the wind turbine node.
        blades: The blade field.
        capacity: The capacity field.
        datasheets: The datasheet field.
        description: Description of the instance
        metmast: The metmast field.
        nacelle: The nacelle field.
        name: Name of the instance
        power_curve: The power curve field.
        rotor: The rotor field.
        windfarm: The windfarm field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "WindTurbine", "1")
    blades: Optional[list[BladeGraphQL]] = Field(default=None, repr=False)
    capacity: Optional[float] = None
    datasheets: Optional[list[DataSheetGraphQL]] = Field(default=None, repr=False)
    description: Optional[str] = None
    metmast: Optional[list[DistanceGraphQL]] = Field(default=None, repr=False)
    nacelle: Optional[NacelleGraphQL] = Field(default=None, repr=False)
    name: Optional[str] = None
    power_curve: Optional[SequenceGraphQL] = Field(None, alias="powerCurve")
    rotor: Optional[RotorGraphQL] = Field(default=None, repr=False)
    windfarm: Optional[str] = None

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

    @field_validator("blades", "datasheets", "metmast", "nacelle", "rotor", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> WindTurbine:
        """Convert this GraphQL format of wind turbine to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return WindTurbine(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            blades=[blade.as_read() for blade in self.blades] if self.blades is not None else None,
            capacity=self.capacity,
            datasheets=[datasheet.as_read() for datasheet in self.datasheets] if self.datasheets is not None else None,
            description=self.description,
            metmast=[metmast.as_read() for metmast in self.metmast] if self.metmast is not None else None,
            nacelle=self.nacelle.as_read() if isinstance(self.nacelle, GraphQLCore) else self.nacelle,
            name=self.name,
            power_curve=self.power_curve.as_read() if self.power_curve else None,
            rotor=self.rotor.as_read() if isinstance(self.rotor, GraphQLCore) else self.rotor,
            windfarm=self.windfarm,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> WindTurbineWrite:
        """Convert this GraphQL format of wind turbine to the writing format."""
        return WindTurbineWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            blades=[blade.as_write() for blade in self.blades] if self.blades is not None else None,
            capacity=self.capacity,
            datasheets=[datasheet.as_write() for datasheet in self.datasheets] if self.datasheets is not None else None,
            description=self.description,
            metmast=[metmast.as_write() for metmast in self.metmast] if self.metmast is not None else None,
            nacelle=self.nacelle.as_write() if isinstance(self.nacelle, GraphQLCore) else self.nacelle,
            name=self.name,
            power_curve=self.power_curve.as_write() if self.power_curve else None,
            rotor=self.rotor.as_write() if isinstance(self.rotor, GraphQLCore) else self.rotor,
            windfarm=self.windfarm,
        )


class WindTurbine(GeneratingUnit):
    """This represents the reading version of wind turbine.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the wind turbine.
        data_record: The data record of the wind turbine node.
        blades: The blade field.
        capacity: The capacity field.
        datasheets: The datasheet field.
        description: Description of the instance
        metmast: The metmast field.
        nacelle: The nacelle field.
        name: Name of the instance
        power_curve: The power curve field.
        rotor: The rotor field.
        windfarm: The windfarm field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "WindTurbine", "1")

    node_type: Union[dm.DirectRelationReference, None] = None
    blades: Optional[list[Union[Blade, str, dm.NodeId]]] = Field(default=None, repr=False)
    datasheets: Optional[list[Union[DataSheet, str, dm.NodeId]]] = Field(default=None, repr=False)
    metmast: Optional[list[Distance]] = Field(default=None, repr=False)
    nacelle: Union[Nacelle, str, dm.NodeId, None] = Field(default=None, repr=False)
    power_curve: Union[SequenceRead, str, None] = Field(None, alias="powerCurve")
    rotor: Union[Rotor, str, dm.NodeId, None] = Field(default=None, repr=False)
    windfarm: Optional[str] = None

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> WindTurbineWrite:
        """Convert this read version of wind turbine to the writing version."""
        return WindTurbineWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            blades=(
                [blade.as_write() if isinstance(blade, DomainModel) else blade for blade in self.blades]
                if self.blades is not None
                else None
            ),
            capacity=self.capacity,
            datasheets=(
                [
                    datasheet.as_write() if isinstance(datasheet, DomainModel) else datasheet
                    for datasheet in self.datasheets
                ]
                if self.datasheets is not None
                else None
            ),
            description=self.description,
            metmast=[metmast.as_write() for metmast in self.metmast] if self.metmast is not None else None,
            nacelle=self.nacelle.as_write() if isinstance(self.nacelle, DomainModel) else self.nacelle,
            name=self.name,
            power_curve=(
                self.power_curve.as_write() if isinstance(self.power_curve, CogniteSequence) else self.power_curve
            ),
            rotor=self.rotor.as_write() if isinstance(self.rotor, DomainModel) else self.rotor,
            windfarm=self.windfarm,
        )

    def as_apply(self) -> WindTurbineWrite:
        """Convert this read version of wind turbine to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, WindTurbine],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._blade import Blade
        from ._data_sheet import DataSheet
        from ._distance import Distance
        from ._nacelle import Nacelle
        from ._rotor import Rotor

        for instance in instances.values():
            if (
                isinstance(instance.nacelle, (dm.NodeId, str))
                and (nacelle := nodes_by_id.get(instance.nacelle))
                and isinstance(nacelle, Nacelle)
            ):
                instance.nacelle = nacelle
            if (
                isinstance(instance.rotor, (dm.NodeId, str))
                and (rotor := nodes_by_id.get(instance.rotor))
                and isinstance(rotor, Rotor)
            ):
                instance.rotor = rotor
            if instance.blades:
                new_blades: list[Blade | str | dm.NodeId] = []
                for blade in instance.blades:
                    if isinstance(blade, Blade):
                        new_blades.append(blade)
                    elif (other := nodes_by_id.get(blade)) and isinstance(other, Blade):
                        new_blades.append(other)
                    else:
                        new_blades.append(blade)
                instance.blades = new_blades
            if instance.datasheets:
                new_datasheets: list[DataSheet | str | dm.NodeId] = []
                for datasheet in instance.datasheets:
                    if isinstance(datasheet, DataSheet):
                        new_datasheets.append(datasheet)
                    elif (other := nodes_by_id.get(datasheet)) and isinstance(other, DataSheet):
                        new_datasheets.append(other)
                    else:
                        new_datasheets.append(datasheet)
                instance.datasheets = new_datasheets
            if edges := edges_by_source_node.get(instance.as_id()):
                metmast: list[Distance] = []
                for edge in edges:
                    value: DomainModel | DomainRelation | str | dm.NodeId
                    if isinstance(edge, DomainRelation):
                        value = edge
                    else:
                        other_end: dm.DirectRelationReference = (
                            edge.end_node
                            if edge.start_node.space == instance.space
                            and edge.start_node.external_id == instance.external_id
                            else edge.start_node
                        )
                        destination: dm.NodeId | str = (
                            as_node_id(other_end)
                            if other_end.space != DEFAULT_INSTANCE_SPACE
                            else other_end.external_id
                        )
                        if destination in nodes_by_id:
                            value = nodes_by_id[destination]
                        else:
                            value = destination
                    edge_type = edge.edge_type if isinstance(edge, DomainRelation) else edge.type

                    if edge_type == dm.DirectRelationReference("sp_pygen_power_enterprise", "Distance") and isinstance(
                        value, Distance
                    ):
                        metmast.append(value)
                        if end_node := nodes_by_id.get(as_pygen_node_id(value.end_node)):
                            value.end_node = end_node  # type: ignore[assignment]

                instance.metmast = metmast


class WindTurbineWrite(GeneratingUnitWrite):
    """This represents the writing version of wind turbine.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the wind turbine.
        data_record: The data record of the wind turbine node.
        blades: The blade field.
        capacity: The capacity field.
        datasheets: The datasheet field.
        description: Description of the instance
        metmast: The metmast field.
        nacelle: The nacelle field.
        name: Name of the instance
        power_curve: The power curve field.
        rotor: The rotor field.
        windfarm: The windfarm field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "WindTurbine", "1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    blades: Optional[list[Union[BladeWrite, str, dm.NodeId]]] = Field(default=None, repr=False)
    datasheets: Optional[list[Union[DataSheetWrite, str, dm.NodeId]]] = Field(default=None, repr=False)
    metmast: Optional[list[DistanceWrite]] = Field(default=None, repr=False)
    nacelle: Union[NacelleWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    power_curve: Union[SequenceWrite, str, None] = Field(None, alias="powerCurve")
    rotor: Union[RotorWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    windfarm: Optional[str] = None

    @field_validator("blades", "datasheets", "metmast", "nacelle", "rotor", mode="before")
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

        if self.blades is not None:
            properties["blades"] = [
                {
                    "space": self.space if isinstance(blade, str) else blade.space,
                    "externalId": blade if isinstance(blade, str) else blade.external_id,
                }
                for blade in self.blades or []
            ]

        if self.capacity is not None or write_none:
            properties["capacity"] = self.capacity

        if self.datasheets is not None:
            properties["datasheets"] = [
                {
                    "space": self.space if isinstance(datasheet, str) else datasheet.space,
                    "externalId": datasheet if isinstance(datasheet, str) else datasheet.external_id,
                }
                for datasheet in self.datasheets or []
            ]

        if self.description is not None or write_none:
            properties["description"] = self.description

        if self.nacelle is not None:
            properties["nacelle"] = {
                "space": self.space if isinstance(self.nacelle, str) else self.nacelle.space,
                "externalId": self.nacelle if isinstance(self.nacelle, str) else self.nacelle.external_id,
            }

        if self.name is not None or write_none:
            properties["name"] = self.name

        if self.power_curve is not None or write_none:
            properties["powerCurve"] = (
                self.power_curve
                if isinstance(self.power_curve, str) or self.power_curve is None
                else self.power_curve.external_id
            )

        if self.rotor is not None:
            properties["rotor"] = {
                "space": self.space if isinstance(self.rotor, str) else self.rotor.space,
                "externalId": self.rotor if isinstance(self.rotor, str) else self.rotor.external_id,
            }

        if self.windfarm is not None or write_none:
            properties["windfarm"] = self.windfarm

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

        for metmast in self.metmast or []:
            if isinstance(metmast, DomainRelationWrite):
                other_resources = metmast._to_instances_write(
                    cache,
                    self,
                    dm.DirectRelationReference("sp_pygen_power_enterprise", "Distance"),
                )
                resources.extend(other_resources)

        if isinstance(self.nacelle, DomainModelWrite):
            other_resources = self.nacelle._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.rotor, DomainModelWrite):
            other_resources = self.rotor._to_instances_write(cache)
            resources.extend(other_resources)

        for blade in self.blades or []:
            if isinstance(blade, DomainModelWrite):
                other_resources = blade._to_instances_write(cache)
                resources.extend(other_resources)

        for datasheet in self.datasheets or []:
            if isinstance(datasheet, DomainModelWrite):
                other_resources = datasheet._to_instances_write(cache)
                resources.extend(other_resources)

        if isinstance(self.power_curve, CogniteSequenceWrite):
            resources.sequences.append(self.power_curve)

        return resources


class WindTurbineApply(WindTurbineWrite):
    def __new__(cls, *args, **kwargs) -> WindTurbineApply:
        warnings.warn(
            "WindTurbineApply is deprecated and will be removed in v1.0. Use WindTurbineWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "WindTurbine.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class WindTurbineList(DomainModelList[WindTurbine]):
    """List of wind turbines in the read version."""

    _INSTANCE = WindTurbine

    def as_write(self) -> WindTurbineWriteList:
        """Convert these read versions of wind turbine to the writing versions."""
        return WindTurbineWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> WindTurbineWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @property
    def blades(self) -> BladeList:
        from ._blade import Blade, BladeList

        return BladeList([item for items in self.data for item in items.blades or [] if isinstance(item, Blade)])

    @property
    def datasheets(self) -> DataSheetList:
        from ._data_sheet import DataSheet, DataSheetList

        return DataSheetList(
            [item for items in self.data for item in items.datasheets or [] if isinstance(item, DataSheet)]
        )

    @property
    def metmast(self) -> DistanceList:
        from ._distance import Distance, DistanceList

        return DistanceList([item for items in self.data for item in items.metmast or [] if isinstance(item, Distance)])

    @property
    def nacelle(self) -> NacelleList:
        from ._nacelle import Nacelle, NacelleList

        return NacelleList([item.nacelle for item in self.data if isinstance(item.nacelle, Nacelle)])

    @property
    def rotor(self) -> RotorList:
        from ._rotor import Rotor, RotorList

        return RotorList([item.rotor for item in self.data if isinstance(item.rotor, Rotor)])


class WindTurbineWriteList(DomainModelWriteList[WindTurbineWrite]):
    """List of wind turbines in the writing version."""

    _INSTANCE = WindTurbineWrite

    @property
    def blades(self) -> BladeWriteList:
        from ._blade import BladeWrite, BladeWriteList

        return BladeWriteList(
            [item for items in self.data for item in items.blades or [] if isinstance(item, BladeWrite)]
        )

    @property
    def datasheets(self) -> DataSheetWriteList:
        from ._data_sheet import DataSheetWrite, DataSheetWriteList

        return DataSheetWriteList(
            [item for items in self.data for item in items.datasheets or [] if isinstance(item, DataSheetWrite)]
        )

    @property
    def metmast(self) -> DistanceWriteList:
        from ._distance import DistanceWrite, DistanceWriteList

        return DistanceWriteList(
            [item for items in self.data for item in items.metmast or [] if isinstance(item, DistanceWrite)]
        )

    @property
    def nacelle(self) -> NacelleWriteList:
        from ._nacelle import NacelleWrite, NacelleWriteList

        return NacelleWriteList([item.nacelle for item in self.data if isinstance(item.nacelle, NacelleWrite)])

    @property
    def rotor(self) -> RotorWriteList:
        from ._rotor import RotorWrite, RotorWriteList

        return RotorWriteList([item.rotor for item in self.data if isinstance(item.rotor, RotorWrite)])


class WindTurbineApplyList(WindTurbineWriteList): ...


def _create_wind_turbine_filter(
    view_id: dm.ViewId,
    blades: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    min_capacity: float | None = None,
    max_capacity: float | None = None,
    datasheets: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    description: str | list[str] | None = None,
    description_prefix: str | None = None,
    nacelle: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    rotor: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    windfarm: str | list[str] | None = None,
    windfarm_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(blades, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(blades):
        filters.append(dm.filters.Equals(view_id.as_property_ref("blades"), value=as_instance_dict_id(blades)))
    if blades and isinstance(blades, Sequence) and not isinstance(blades, str) and not is_tuple_id(blades):
        filters.append(
            dm.filters.In(view_id.as_property_ref("blades"), values=[as_instance_dict_id(item) for item in blades])
        )
    if min_capacity is not None or max_capacity is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("capacity"), gte=min_capacity, lte=max_capacity))
    if isinstance(datasheets, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(datasheets):
        filters.append(dm.filters.Equals(view_id.as_property_ref("datasheets"), value=as_instance_dict_id(datasheets)))
    if (
        datasheets
        and isinstance(datasheets, Sequence)
        and not isinstance(datasheets, str)
        and not is_tuple_id(datasheets)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("datasheets"), values=[as_instance_dict_id(item) for item in datasheets]
            )
        )
    if isinstance(description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("description"), value=description))
    if description and isinstance(description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("description"), values=description))
    if description_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("description"), value=description_prefix))
    if isinstance(nacelle, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(nacelle):
        filters.append(dm.filters.Equals(view_id.as_property_ref("nacelle"), value=as_instance_dict_id(nacelle)))
    if nacelle and isinstance(nacelle, Sequence) and not isinstance(nacelle, str) and not is_tuple_id(nacelle):
        filters.append(
            dm.filters.In(view_id.as_property_ref("nacelle"), values=[as_instance_dict_id(item) for item in nacelle])
        )
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if isinstance(rotor, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(rotor):
        filters.append(dm.filters.Equals(view_id.as_property_ref("rotor"), value=as_instance_dict_id(rotor)))
    if rotor and isinstance(rotor, Sequence) and not isinstance(rotor, str) and not is_tuple_id(rotor):
        filters.append(
            dm.filters.In(view_id.as_property_ref("rotor"), values=[as_instance_dict_id(item) for item in rotor])
        )
    if isinstance(windfarm, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("windfarm"), value=windfarm))
    if windfarm and isinstance(windfarm, list):
        filters.append(dm.filters.In(view_id.as_property_ref("windfarm"), values=windfarm))
    if windfarm_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("windfarm"), value=windfarm_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _WindTurbineQuery(NodeQueryCore[T_DomainModelList, WindTurbineList]):
    _view_id = WindTurbine._view_id
    _result_cls = WindTurbine
    _result_list_cls_end = WindTurbineList

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
        from ._blade import _BladeQuery
        from ._data_sheet import _DataSheetQuery
        from ._distance import _DistanceQuery
        from ._metmast import _MetmastQuery
        from ._nacelle import _NacelleQuery
        from ._rotor import _RotorQuery

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

        if _BladeQuery not in created_types:
            self.blades = _BladeQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("blades"),
                    direction="outwards",
                ),
                connection_name="blades",
            )

        if _DataSheetQuery not in created_types:
            self.datasheets = _DataSheetQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("datasheets"),
                    direction="outwards",
                ),
                connection_name="datasheets",
            )

        if _DistanceQuery not in created_types:
            self.metmast = _DistanceQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                _MetmastQuery,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="metmast",
            )

        if _NacelleQuery not in created_types:
            self.nacelle = _NacelleQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("nacelle"),
                    direction="outwards",
                ),
                connection_name="nacelle",
            )

        if _RotorQuery not in created_types:
            self.rotor = _RotorQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("rotor"),
                    direction="outwards",
                ),
                connection_name="rotor",
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.capacity = FloatFilter(self, self._view_id.as_property_ref("capacity"))
        self.description = StringFilter(self, self._view_id.as_property_ref("description"))
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.windfarm = StringFilter(self, self._view_id.as_property_ref("windfarm"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.capacity,
                self.description,
                self.name,
                self.windfarm,
            ]
        )

    def list_wind_turbine(self, limit: int = DEFAULT_QUERY_LIMIT) -> WindTurbineList:
        return self._list(limit=limit)


class WindTurbineQuery(_WindTurbineQuery[WindTurbineList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, WindTurbineList)
