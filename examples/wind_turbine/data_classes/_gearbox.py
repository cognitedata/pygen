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
    "Gearbox",
    "GearboxWrite",
    "GearboxList",
    "GearboxWriteList",
    "GearboxGraphQL",
]


GearboxTextFields = Literal["external_id",]
GearboxFields = Literal["external_id",]

_GEARBOX_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
}


class GearboxGraphQL(GraphQLCore):
    """This represents the reading version of gearbox, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the gearbox.
        data_record: The data record of the gearbox node.
        displacement_x: The displacement x field.
        displacement_y: The displacement y field.
        displacement_z: The displacement z field.
        nacelle: The nacelle field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "Gearbox", "1")
    displacement_x: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)
    displacement_y: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)
    displacement_z: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)
    nacelle: Optional[NacelleGraphQL] = Field(default=None, repr=False)

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

    @field_validator("displacement_x", "displacement_y", "displacement_z", "nacelle", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> Gearbox:
        """Convert this GraphQL format of gearbox to the reading format."""
        return Gearbox.model_validate(as_read_args(self))

    def as_write(self) -> GearboxWrite:
        """Convert this GraphQL format of gearbox to the writing format."""
        return GearboxWrite.model_validate(as_write_args(self))


class Gearbox(DomainModel):
    """This represents the reading version of gearbox.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the gearbox.
        data_record: The data record of the gearbox node.
        displacement_x: The displacement x field.
        displacement_y: The displacement y field.
        displacement_z: The displacement z field.
        nacelle: The nacelle field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "Gearbox", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    displacement_x: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(default=None, repr=False)
    displacement_y: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(default=None, repr=False)
    displacement_z: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(default=None, repr=False)
    nacelle: Optional[Nacelle] = Field(default=None, repr=False)

    @field_validator("displacement_x", "displacement_y", "displacement_z", "nacelle", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    def as_write(self) -> GearboxWrite:
        """Convert this read version of gearbox to the writing version."""
        return GearboxWrite.model_validate(as_write_args(self))


class GearboxWrite(DomainModelWrite):
    """This represents the writing version of gearbox.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the gearbox.
        data_record: The data record of the gearbox node.
        displacement_x: The displacement x field.
        displacement_y: The displacement y field.
        displacement_z: The displacement z field.
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "displacement_x",
        "displacement_y",
        "displacement_z",
    )
    _direct_relations: ClassVar[tuple[str, ...]] = (
        "displacement_x",
        "displacement_y",
        "displacement_z",
    )

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "Gearbox", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    displacement_x: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    displacement_y: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    displacement_z: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(default=None, repr=False)

    @field_validator("displacement_x", "displacement_y", "displacement_z", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class GearboxList(DomainModelList[Gearbox]):
    """List of gearboxes in the read version."""

    _INSTANCE = Gearbox

    def as_write(self) -> GearboxWriteList:
        """Convert these read versions of gearbox to the writing versions."""
        return GearboxWriteList([node.as_write() for node in self.data])

    @property
    def displacement_x(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [item.displacement_x for item in self.data if isinstance(item.displacement_x, SensorTimeSeries)]
        )

    @property
    def displacement_y(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [item.displacement_y for item in self.data if isinstance(item.displacement_y, SensorTimeSeries)]
        )

    @property
    def displacement_z(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [item.displacement_z for item in self.data if isinstance(item.displacement_z, SensorTimeSeries)]
        )

    @property
    def nacelle(self) -> NacelleList:
        from ._nacelle import Nacelle, NacelleList

        return NacelleList([item.nacelle for item in self.data if isinstance(item.nacelle, Nacelle)])


class GearboxWriteList(DomainModelWriteList[GearboxWrite]):
    """List of gearboxes in the writing version."""

    _INSTANCE = GearboxWrite

    @property
    def displacement_x(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [item.displacement_x for item in self.data if isinstance(item.displacement_x, SensorTimeSeriesWrite)]
        )

    @property
    def displacement_y(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [item.displacement_y for item in self.data if isinstance(item.displacement_y, SensorTimeSeriesWrite)]
        )

    @property
    def displacement_z(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [item.displacement_z for item in self.data if isinstance(item.displacement_z, SensorTimeSeriesWrite)]
        )


def _create_gearbox_filter(
    view_id: dm.ViewId,
    displacement_x: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    displacement_y: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    displacement_z: (
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
    if isinstance(displacement_x, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(displacement_x):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("displacement_x"), value=as_instance_dict_id(displacement_x))
        )
    if (
        displacement_x
        and isinstance(displacement_x, Sequence)
        and not isinstance(displacement_x, str)
        and not is_tuple_id(displacement_x)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("displacement_x"), values=[as_instance_dict_id(item) for item in displacement_x]
            )
        )
    if isinstance(displacement_y, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(displacement_y):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("displacement_y"), value=as_instance_dict_id(displacement_y))
        )
    if (
        displacement_y
        and isinstance(displacement_y, Sequence)
        and not isinstance(displacement_y, str)
        and not is_tuple_id(displacement_y)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("displacement_y"), values=[as_instance_dict_id(item) for item in displacement_y]
            )
        )
    if isinstance(displacement_z, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(displacement_z):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("displacement_z"), value=as_instance_dict_id(displacement_z))
        )
    if (
        displacement_z
        and isinstance(displacement_z, Sequence)
        and not isinstance(displacement_z, str)
        and not is_tuple_id(displacement_z)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("displacement_z"), values=[as_instance_dict_id(item) for item in displacement_z]
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


class _GearboxQuery(NodeQueryCore[T_DomainModelList, GearboxList]):
    _view_id = Gearbox._view_id
    _result_cls = Gearbox
    _result_list_cls_end = GearboxList

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
            self.displacement_x = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("displacement_x"),
                    direction="outwards",
                ),
                connection_name="displacement_x",
                connection_property=ViewPropertyId(self._view_id, "displacement_x"),
            )

        if _SensorTimeSeriesQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.displacement_y = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("displacement_y"),
                    direction="outwards",
                ),
                connection_name="displacement_y",
                connection_property=ViewPropertyId(self._view_id, "displacement_y"),
            )

        if _SensorTimeSeriesQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.displacement_z = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("displacement_z"),
                    direction="outwards",
                ),
                connection_name="displacement_z",
                connection_property=ViewPropertyId(self._view_id, "displacement_z"),
            )

        if _NacelleQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.nacelle = _NacelleQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=dm.ViewId("sp_pygen_power", "Nacelle", "1").as_property_ref("gearbox"),
                    direction="inwards",
                ),
                connection_name="nacelle",
                connection_property=ViewPropertyId(self._view_id, "nacelle"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.displacement_x_filter = DirectRelationFilter(self, self._view_id.as_property_ref("displacement_x"))
        self.displacement_y_filter = DirectRelationFilter(self, self._view_id.as_property_ref("displacement_y"))
        self.displacement_z_filter = DirectRelationFilter(self, self._view_id.as_property_ref("displacement_z"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.displacement_x_filter,
                self.displacement_y_filter,
                self.displacement_z_filter,
            ]
        )

    def list_gearbox(self, limit: int = DEFAULT_QUERY_LIMIT) -> GearboxList:
        return self._list(limit=limit)


class GearboxQuery(_GearboxQuery[GearboxList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, GearboxList)
