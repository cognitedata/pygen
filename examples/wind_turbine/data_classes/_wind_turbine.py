from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from cognite.client.data_classes import (
    Sequence as CogniteSequence,
    SequenceWrite as CogniteSequenceWrite,
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
    SequenceRead,
    SequenceWrite,
    SequenceGraphQL,
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
    FloatFilter,
)
from wind_turbine.data_classes._generating_unit import GeneratingUnit, GeneratingUnitWrite

if TYPE_CHECKING:
    from wind_turbine.data_classes._blade import Blade, BladeList, BladeGraphQL, BladeWrite, BladeWriteList
    from wind_turbine.data_classes._data_sheet import (
        DataSheet,
        DataSheetList,
        DataSheetGraphQL,
        DataSheetWrite,
        DataSheetWriteList,
    )
    from wind_turbine.data_classes._distance import (
        Distance,
        DistanceList,
        DistanceGraphQL,
        DistanceWrite,
        DistanceWriteList,
    )
    from wind_turbine.data_classes._nacelle import Nacelle, NacelleList, NacelleGraphQL, NacelleWrite, NacelleWriteList
    from wind_turbine.data_classes._rotor import Rotor, RotorList, RotorGraphQL, RotorWrite, RotorWriteList


__all__ = [
    "WindTurbine",
    "WindTurbineWrite",
    "WindTurbineList",
    "WindTurbineWriteList",
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

    def as_read(self) -> WindTurbine:
        """Convert this GraphQL format of wind turbine to the reading format."""
        return WindTurbine.model_validate(as_read_args(self))

    def as_write(self) -> WindTurbineWrite:
        """Convert this GraphQL format of wind turbine to the writing format."""
        return WindTurbineWrite.model_validate(as_write_args(self))


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

    @field_validator("nacelle", "rotor", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    @field_validator("blades", "datasheets", "metmast", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return [parse_single_connection(item, info.field_name) for item in value]

    def as_write(self) -> WindTurbineWrite:
        """Convert this read version of wind turbine to the writing version."""
        return WindTurbineWrite.model_validate(as_write_args(self))


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

    _container_fields: ClassVar[tuple[str, ...]] = (
        "blades",
        "capacity",
        "datasheets",
        "description",
        "nacelle",
        "name",
        "power_curve",
        "rotor",
        "windfarm",
    )
    _outwards_edges: ClassVar[tuple[tuple[str, dm.DirectRelationReference], ...]] = (
        ("metmast", dm.DirectRelationReference("sp_pygen_power_enterprise", "Distance")),
    )
    _direct_relations: ClassVar[tuple[str, ...]] = (
        "blades",
        "datasheets",
        "nacelle",
        "rotor",
    )

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


class WindTurbineList(DomainModelList[WindTurbine]):
    """List of wind turbines in the read version."""

    _INSTANCE = WindTurbine

    def as_write(self) -> WindTurbineWriteList:
        """Convert these read versions of wind turbine to the writing versions."""
        return WindTurbineWriteList([node.as_write() for node in self.data])

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
        expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_property: ViewPropertyId | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
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
            connection_property,
            connection_type,
            reverse_expression,
        )

        if _BladeQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
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
                connection_property=ViewPropertyId(self._view_id, "blades"),
            )

        if _DataSheetQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
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
                connection_property=ViewPropertyId(self._view_id, "datasheets"),
            )

        if _DistanceQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
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
                connection_property=ViewPropertyId(self._view_id, "metmast"),
            )

        if _NacelleQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
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
                connection_property=ViewPropertyId(self._view_id, "nacelle"),
            )

        if _RotorQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
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
                connection_property=ViewPropertyId(self._view_id, "rotor"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.capacity = FloatFilter(self, self._view_id.as_property_ref("capacity"))
        self.description = StringFilter(self, self._view_id.as_property_ref("description"))
        self.nacelle_filter = DirectRelationFilter(self, self._view_id.as_property_ref("nacelle"))
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.rotor_filter = DirectRelationFilter(self, self._view_id.as_property_ref("rotor"))
        self.windfarm = StringFilter(self, self._view_id.as_property_ref("windfarm"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.capacity,
                self.description,
                self.nacelle_filter,
                self.name,
                self.rotor_filter,
                self.windfarm,
            ]
        )

    def list_wind_turbine(self, limit: int = DEFAULT_QUERY_LIMIT) -> WindTurbineList:
        return self._list(limit=limit)


class WindTurbineQuery(_WindTurbineQuery[WindTurbineList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, WindTurbineList)
