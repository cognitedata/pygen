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
    "SolarPanelApply",
    "SolarPanelList",
    "SolarPanelWriteList",
    "SolarPanelApplyList",
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> SolarPanel:
        """Convert this GraphQL format of solar panel to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return SolarPanel(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            capacity=self.capacity,
            description=self.description,
            efficiency=self.efficiency.as_read() if isinstance(self.efficiency, GraphQLCore) else self.efficiency,
            name=self.name,
            orientation=self.orientation.as_read() if isinstance(self.orientation, GraphQLCore) else self.orientation,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> SolarPanelWrite:
        """Convert this GraphQL format of solar panel to the writing format."""
        return SolarPanelWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            capacity=self.capacity,
            description=self.description,
            efficiency=self.efficiency.as_write() if isinstance(self.efficiency, GraphQLCore) else self.efficiency,
            name=self.name,
            orientation=self.orientation.as_write() if isinstance(self.orientation, GraphQLCore) else self.orientation,
        )


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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> SolarPanelWrite:
        """Convert this read version of solar panel to the writing version."""
        return SolarPanelWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            capacity=self.capacity,
            description=self.description,
            efficiency=self.efficiency.as_write() if isinstance(self.efficiency, DomainModel) else self.efficiency,
            name=self.name,
            orientation=self.orientation.as_write() if isinstance(self.orientation, DomainModel) else self.orientation,
        )

    def as_apply(self) -> SolarPanelWrite:
        """Convert this read version of solar panel to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, SolarPanel],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._sensor_time_series import SensorTimeSeries

        for instance in instances.values():
            if (
                isinstance(instance.efficiency, dm.NodeId | str)
                and (efficiency := nodes_by_id.get(instance.efficiency))
                and isinstance(efficiency, SensorTimeSeries)
            ):
                instance.efficiency = efficiency
            if (
                isinstance(instance.orientation, dm.NodeId | str)
                and (orientation := nodes_by_id.get(instance.orientation))
                and isinstance(orientation, SensorTimeSeries)
            ):
                instance.orientation = orientation


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

        if self.capacity is not None or write_none:
            properties["capacity"] = self.capacity

        if self.description is not None or write_none:
            properties["description"] = self.description

        if self.efficiency is not None:
            properties["efficiency"] = {
                "space": self.space if isinstance(self.efficiency, str) else self.efficiency.space,
                "externalId": self.efficiency if isinstance(self.efficiency, str) else self.efficiency.external_id,
            }

        if self.name is not None or write_none:
            properties["name"] = self.name

        if self.orientation is not None:
            properties["orientation"] = {
                "space": self.space if isinstance(self.orientation, str) else self.orientation.space,
                "externalId": self.orientation if isinstance(self.orientation, str) else self.orientation.external_id,
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

        if isinstance(self.efficiency, DomainModelWrite):
            other_resources = self.efficiency._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.orientation, DomainModelWrite):
            other_resources = self.orientation._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class SolarPanelApply(SolarPanelWrite):
    def __new__(cls, *args, **kwargs) -> SolarPanelApply:
        warnings.warn(
            "SolarPanelApply is deprecated and will be removed in v1.0. "
            "Use SolarPanelWrite instead. "
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "SolarPanel.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class SolarPanelList(DomainModelList[SolarPanel]):
    """List of solar panels in the read version."""

    _INSTANCE = SolarPanel

    def as_write(self) -> SolarPanelWriteList:
        """Convert these read versions of solar panel to the writing versions."""
        return SolarPanelWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> SolarPanelWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

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


class SolarPanelApplyList(SolarPanelWriteList): ...


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
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
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
            connection_type,
            reverse_expression,
        )

        if _SensorTimeSeriesQuery not in created_types:
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
            )

        if _SensorTimeSeriesQuery not in created_types:
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
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.capacity = FloatFilter(self, self._view_id.as_property_ref("capacity"))
        self.description = StringFilter(self, self._view_id.as_property_ref("description"))
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.capacity,
                self.description,
                self.name,
            ]
        )

    def list_solar_panel(self, limit: int = DEFAULT_QUERY_LIMIT) -> SolarPanelList:
        return self._list(limit=limit)


class SolarPanelQuery(_SolarPanelQuery[SolarPanelList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, SolarPanelList)
