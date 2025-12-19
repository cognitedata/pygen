from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator, ValidationInfo

from wind_turbine.config import global_config
from wind_turbine.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    DataPointsAPI,
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
    BooleanFilter,
)


__all__ = [
    "SensorTimeSeries",
    "SensorTimeSeriesWrite",
    "SensorTimeSeriesList",
    "SensorTimeSeriesWriteList",
    "SensorTimeSeriesFields",
    "SensorTimeSeriesTextFields",
    "SensorTimeSeriesGraphQL",
]


SensorTimeSeriesTextFields = Literal[
    "external_id", "aliases", "concept_id", "description", "name", "source_unit", "standard_name"
]
SensorTimeSeriesFields = Literal[
    "external_id", "aliases", "concept_id", "description", "is_step", "name", "source_unit", "standard_name", "type_"
]

_SENSORTIMESERIES_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "aliases": "aliases",
    "concept_id": "conceptId",
    "description": "description",
    "is_step": "isStep",
    "name": "name",
    "source_unit": "sourceUnit",
    "standard_name": "standardName",
    "type_": "type",
}


class SensorTimeSeriesGraphQL(GraphQLCore):
    """This represents the reading version of sensor time series, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the sensor time series.
        data_record: The data record of the sensor time series node.
        aliases: Alternative names for the node
        concept_id: The concept ID of the time series.
        description: Description of the instance
        is_step: Specifies whether the time series is a step time series or not.
        name: Name of the instance
        source_unit: The unit specified in the source system.
        standard_name: The standard name of the time series.
        type_: Specifies the data type of the data points.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "SensorTimeSeries", "1")
    aliases: Optional[list[str]] = None
    concept_id: Optional[str] = Field(None, alias="conceptId")
    description: Optional[str] = None
    is_step: Optional[bool] = Field(None, alias="isStep")
    name: Optional[str] = None
    source_unit: Optional[str] = Field(None, alias="sourceUnit")
    standard_name: Optional[str] = Field(None, alias="standardName")
    type_: Optional[Literal["numeric", "string"]] = Field(None, alias="type")

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

    def as_read(self) -> SensorTimeSeries:
        """Convert this GraphQL format of sensor time series to the reading format."""
        return SensorTimeSeries.model_validate(as_read_args(self))

    def as_write(self) -> SensorTimeSeriesWrite:
        """Convert this GraphQL format of sensor time series to the writing format."""
        return SensorTimeSeriesWrite.model_validate(as_write_args(self))


class SensorTimeSeries(DomainModel):
    """This represents the reading version of sensor time series.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the sensor time series.
        data_record: The data record of the sensor time series node.
        aliases: Alternative names for the node
        concept_id: The concept ID of the time series.
        description: Description of the instance
        is_step: Specifies whether the time series is a step time series or not.
        name: Name of the instance
        source_unit: The unit specified in the source system.
        standard_name: The standard name of the time series.
        type_: Specifies the data type of the data points.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "SensorTimeSeries", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    aliases: Optional[list[str]] = None
    concept_id: Optional[str] = Field(None, alias="conceptId")
    description: Optional[str] = None
    is_step: bool = Field(alias="isStep")
    name: Optional[str] = None
    source_unit: Optional[str] = Field(None, alias="sourceUnit")
    standard_name: Optional[str] = Field(None, alias="standardName")
    type_: Literal["numeric", "string"] | str = Field(alias="type")

    def as_write(self) -> SensorTimeSeriesWrite:
        """Convert this read version of sensor time series to the writing version."""
        return SensorTimeSeriesWrite.model_validate(as_write_args(self))


class SensorTimeSeriesWrite(DomainModelWrite):
    """This represents the writing version of sensor time series.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the sensor time series.
        data_record: The data record of the sensor time series node.
        aliases: Alternative names for the node
        concept_id: The concept ID of the time series.
        description: Description of the instance
        is_step: Specifies whether the time series is a step time series or not.
        name: Name of the instance
        source_unit: The unit specified in the source system.
        standard_name: The standard name of the time series.
        type_: Specifies the data type of the data points.
    """

    _container_fields: ClassVar[tuple[str, ...]] = (
        "aliases",
        "concept_id",
        "description",
        "is_step",
        "name",
        "source_unit",
        "standard_name",
        "type_",
    )

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("sp_pygen_power", "SensorTimeSeries", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    aliases: Optional[list[str]] = None
    concept_id: Optional[str] = Field(None, alias="conceptId")
    description: Optional[str] = None
    is_step: bool = Field(alias="isStep")
    name: Optional[str] = None
    source_unit: Optional[str] = Field(None, alias="sourceUnit")
    standard_name: Optional[str] = Field(None, alias="standardName")
    type_: Literal["numeric", "string"] = Field(alias="type")


class SensorTimeSeriesList(DomainModelList[SensorTimeSeries]):
    """List of sensor time series in the read version."""

    _INSTANCE = SensorTimeSeries

    def as_write(self) -> SensorTimeSeriesWriteList:
        """Convert these read versions of sensor time series to the writing versions."""
        return SensorTimeSeriesWriteList([node.as_write() for node in self.data])


class SensorTimeSeriesWriteList(DomainModelWriteList[SensorTimeSeriesWrite]):
    """List of sensor time series in the writing version."""

    _INSTANCE = SensorTimeSeriesWrite


def _create_sensor_time_series_filter(
    view_id: dm.ViewId,
    concept_id: str | list[str] | None = None,
    concept_id_prefix: str | None = None,
    description: str | list[str] | None = None,
    description_prefix: str | None = None,
    is_step: bool | None = None,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    source_unit: str | list[str] | None = None,
    source_unit_prefix: str | None = None,
    standard_name: str | list[str] | None = None,
    standard_name_prefix: str | None = None,
    type_: Literal["numeric", "string"] | list[Literal["numeric", "string"]] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(concept_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("conceptId"), value=concept_id))
    if concept_id and isinstance(concept_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("conceptId"), values=concept_id))
    if concept_id_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("conceptId"), value=concept_id_prefix))
    if isinstance(description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("description"), value=description))
    if description and isinstance(description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("description"), values=description))
    if description_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("description"), value=description_prefix))
    if isinstance(is_step, bool):
        filters.append(dm.filters.Equals(view_id.as_property_ref("isStep"), value=is_step))
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if isinstance(source_unit, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("sourceUnit"), value=source_unit))
    if source_unit and isinstance(source_unit, list):
        filters.append(dm.filters.In(view_id.as_property_ref("sourceUnit"), values=source_unit))
    if source_unit_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("sourceUnit"), value=source_unit_prefix))
    if isinstance(standard_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("standardName"), value=standard_name))
    if standard_name and isinstance(standard_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("standardName"), values=standard_name))
    if standard_name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("standardName"), value=standard_name_prefix))
    if isinstance(type_, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("type"), value=type_))
    if type_ and isinstance(type_, list):
        filters.append(dm.filters.In(view_id.as_property_ref("type"), values=type_))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _SensorTimeSeriesQuery(NodeQueryCore[T_DomainModelList, SensorTimeSeriesList]):
    _view_id = SensorTimeSeries._view_id
    _result_cls = SensorTimeSeries
    _result_list_cls_end = SensorTimeSeriesList

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

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.concept_id = StringFilter(self, self._view_id.as_property_ref("conceptId"))
        self.description = StringFilter(self, self._view_id.as_property_ref("description"))
        self.is_step = BooleanFilter(self, self._view_id.as_property_ref("isStep"))
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.source_unit = StringFilter(self, self._view_id.as_property_ref("sourceUnit"))
        self.standard_name = StringFilter(self, self._view_id.as_property_ref("standardName"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.concept_id,
                self.description,
                self.is_step,
                self.name,
                self.source_unit,
                self.standard_name,
            ]
        )
        self.data = DataPointsAPI(client, lambda limit: self._list(limit=limit).as_node_ids())

    def list_sensor_time_series(self, limit: int = DEFAULT_QUERY_LIMIT) -> SensorTimeSeriesList:
        return self._list(limit=limit)


class SensorTimeSeriesQuery(_SensorTimeSeriesQuery[SensorTimeSeriesList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, SensorTimeSeriesList)
