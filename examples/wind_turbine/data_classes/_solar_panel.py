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
    FloatFilter,
)
from wind_turbine.data_classes._generating_unit import GeneratingUnit, GeneratingUnitWrite

if TYPE_CHECKING:
    from wind_turbine.data_classes._sensor_time_series import (
        SensorTimeSeries,
        SensorTimeSeriesList,
        SensorTimeSeriesGraphQL,
        SensorTimeSeriesWrite,
        SensorTimeSeriesWriteList,
    )


__all__ = [
    "SolarPanel",
    "SolarPanelWrite",
    "SolarPanelList",
    "SolarPanelWriteList",
    "SolarPanelFields",
    "SolarPanelTextFields",
    "SolarPanelGraphQL",
]


SolarPanelTextFields = Literal["external_id", "description", "name"]
SolarPanelFields = Literal["external_id", "capacity", "description", "name"]

_SOLARPANEL_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "capacity": "capacity",
    "description": "description",
    "name": "name",
}


class SolarPanelGraphQL(GraphQLCore):
    """This represents the reading version of solar panel, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the solar panel.
        data_record: The data record of the solar panel node.
        capacity: The capacity field.
        description: Description of the instance
        efficiency: The efficiency field.
        name: Name of the instance
        orientation: The orientation field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "SolarPanel", "1")
    capacity: Optional[float] = None
    description: Optional[str] = None
    efficiency: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)
    name: Optional[str] = None
    orientation: Optional[SensorTimeSeriesGraphQL] = Field(default=None, repr=False)

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

    @field_validator("efficiency", "orientation", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> SolarPanel:
        """Convert this GraphQL format of solar panel to the reading format."""
        return SolarPanel.model_validate(as_read_args(self))

    def as_write(self) -> SolarPanelWrite:
        """Convert this GraphQL format of solar panel to the writing format."""
        return SolarPanelWrite.model_validate(as_write_args(self))


class SolarPanel(GeneratingUnit):
    """This represents the reading version of solar panel.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the solar panel.
        data_record: The data record of the solar panel node.
        capacity: The capacity field.
        description: Description of the instance
        efficiency: The efficiency field.
        name: Name of the instance
        orientation: The orientation field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "SolarPanel", "1")

    node_type: Union[dm.DirectRelationReference, None] = None
    efficiency: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(default=None, repr=False)
    orientation: Union[SensorTimeSeries, str, dm.NodeId, None] = Field(default=None, repr=False)

    @field_validator("efficiency", "orientation", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    def as_write(self) -> SolarPanelWrite:
        """Convert this read version of solar panel to the writing version."""
        return SolarPanelWrite.model_validate(as_write_args(self))


class SolarPanelWrite(GeneratingUnitWrite):
    """This represents the writing version of solar panel.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the solar panel.
        data_record: The data record of the solar panel node.
        capacity: The capacity field.
        description: Description of the instance
        efficiency: The efficiency field.
        name: Name of the instance
        orientation: The orientation field.
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "capacity",
        "description",
        "efficiency",
        "name",
        "orientation",
    )
    _direct_relations: ClassVar[tuple[str, ...]] = (
        "efficiency",
        "orientation",
    )

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "SolarPanel", "1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    efficiency: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    orientation: Union[SensorTimeSeriesWrite, str, dm.NodeId, None] = Field(default=None, repr=False)

    @field_validator("efficiency", "orientation", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class SolarPanelList(DomainModelList[SolarPanel]):
    """List of solar panels in the read version."""

    _INSTANCE = SolarPanel

    def as_write(self) -> SolarPanelWriteList:
        """Convert these read versions of solar panel to the writing versions."""
        return SolarPanelWriteList([node.as_write() for node in self.data])

    @property
    def efficiency(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [item.efficiency for item in self.data if isinstance(item.efficiency, SensorTimeSeries)]
        )

    @property
    def orientation(self) -> SensorTimeSeriesList:
        from ._sensor_time_series import SensorTimeSeries, SensorTimeSeriesList

        return SensorTimeSeriesList(
            [item.orientation for item in self.data if isinstance(item.orientation, SensorTimeSeries)]
        )


class SolarPanelWriteList(DomainModelWriteList[SolarPanelWrite]):
    """List of solar panels in the writing version."""

    _INSTANCE = SolarPanelWrite

    @property
    def efficiency(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [item.efficiency for item in self.data if isinstance(item.efficiency, SensorTimeSeriesWrite)]
        )

    @property
    def orientation(self) -> SensorTimeSeriesWriteList:
        from ._sensor_time_series import SensorTimeSeriesWrite, SensorTimeSeriesWriteList

        return SensorTimeSeriesWriteList(
            [item.orientation for item in self.data if isinstance(item.orientation, SensorTimeSeriesWrite)]
        )


def _create_solar_panel_filter(
    view_id: dm.ViewId,
    min_capacity: float | None = None,
    max_capacity: float | None = None,
    description: str | list[str] | None = None,
    description_prefix: str | None = None,
    efficiency: (
        str
        | tuple[str, str]
        | dm.NodeId
        | dm.DirectRelationReference
        | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
        | None
    ) = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    orientation: (
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
    if min_capacity is not None or max_capacity is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("capacity"), gte=min_capacity, lte=max_capacity))
    if isinstance(description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("description"), value=description))
    if description and isinstance(description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("description"), values=description))
    if description_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("description"), value=description_prefix))
    if isinstance(efficiency, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(efficiency):
        filters.append(dm.filters.Equals(view_id.as_property_ref("efficiency"), value=as_instance_dict_id(efficiency)))
    if (
        efficiency
        and isinstance(efficiency, Sequence)
        and not isinstance(efficiency, str)
        and not is_tuple_id(efficiency)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("efficiency"), values=[as_instance_dict_id(item) for item in efficiency]
            )
        )
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if isinstance(orientation, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(orientation):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("orientation"), value=as_instance_dict_id(orientation))
        )
    if (
        orientation
        and isinstance(orientation, Sequence)
        and not isinstance(orientation, str)
        and not is_tuple_id(orientation)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("orientation"), values=[as_instance_dict_id(item) for item in orientation]
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


class _SolarPanelQuery(NodeQueryCore[T_DomainModelList, SolarPanelList]):
    _view_id = SolarPanel._view_id
    _result_cls = SolarPanel
    _result_list_cls_end = SolarPanelList

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
            self.efficiency = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("efficiency"),
                    direction="outwards",
                ),
                connection_name="efficiency",
                connection_property=ViewPropertyId(self._view_id, "efficiency"),
            )

        if _SensorTimeSeriesQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.orientation = _SensorTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("orientation"),
                    direction="outwards",
                ),
                connection_name="orientation",
                connection_property=ViewPropertyId(self._view_id, "orientation"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.capacity = FloatFilter(self, self._view_id.as_property_ref("capacity"))
        self.description = StringFilter(self, self._view_id.as_property_ref("description"))
        self.efficiency_filter = DirectRelationFilter(self, self._view_id.as_property_ref("efficiency"))
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.orientation_filter = DirectRelationFilter(self, self._view_id.as_property_ref("orientation"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.capacity,
                self.description,
                self.efficiency_filter,
                self.name,
                self.orientation_filter,
            ]
        )

    def list_solar_panel(self, limit: int = DEFAULT_QUERY_LIMIT) -> SolarPanelList:
        return self._list(limit=limit)


class SolarPanelQuery(_SolarPanelQuery[SolarPanelList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, SolarPanelList)
