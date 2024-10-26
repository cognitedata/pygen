from __future__ import annotations

import datetime
import warnings
from collections.abc import Sequence
from typing import Any, ClassVar, Literal, no_type_check, Optional, Union

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
    StringFilter,
    TimestampFilter,
)


__all__ = [
    "ScenarioInstance",
    "ScenarioInstanceWrite",
    "ScenarioInstanceApply",
    "ScenarioInstanceList",
    "ScenarioInstanceWriteList",
    "ScenarioInstanceApplyList",
    "ScenarioInstanceFields",
    "ScenarioInstanceTextFields",
    "ScenarioInstanceGraphQL",
]


ScenarioInstanceTextFields = Literal[
    "external_id", "aggregation", "country", "market", "price_area", "price_forecast", "scenario"
]
ScenarioInstanceFields = Literal[
    "external_id", "aggregation", "country", "instance", "market", "price_area", "price_forecast", "scenario", "start"
]

_SCENARIOINSTANCE_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "aggregation": "aggregation",
    "country": "country",
    "instance": "instance",
    "market": "market",
    "price_area": "priceArea",
    "price_forecast": "priceForecast",
    "scenario": "scenario",
    "start": "start",
}


class ScenarioInstanceGraphQL(GraphQLCore):
    """This represents the reading version of scenario instance, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the scenario instance.
        data_record: The data record of the scenario instance node.
        aggregation: The aggregation field.
        country: The country field.
        instance: The instance field.
        market: The market field.
        price_area: The price area field.
        price_forecast: The price forecast field.
        scenario: The scenario field.
        start: The start field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("IntegrationTestsImmutable", "ScenarioInstance", "ee2b79fd98b5bb")
    aggregation: Optional[str] = None
    country: Optional[str] = None
    instance: Optional[datetime.datetime] = None
    market: Optional[str] = None
    price_area: Optional[str] = Field(None, alias="priceArea")
    price_forecast: Optional[TimeSeriesGraphQL] = Field(None, alias="priceForecast")
    scenario: Optional[str] = None
    start: Optional[datetime.datetime] = None

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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> ScenarioInstance:
        """Convert this GraphQL format of scenario instance to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return ScenarioInstance(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            aggregation=self.aggregation,
            country=self.country,
            instance=self.instance,
            market=self.market,
            price_area=self.price_area,
            price_forecast=self.price_forecast.as_read() if self.price_forecast else None,
            scenario=self.scenario,
            start=self.start,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ScenarioInstanceWrite:
        """Convert this GraphQL format of scenario instance to the writing format."""
        return ScenarioInstanceWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            aggregation=self.aggregation,
            country=self.country,
            instance=self.instance,
            market=self.market,
            price_area=self.price_area,
            price_forecast=self.price_forecast.as_write() if self.price_forecast else None,
            scenario=self.scenario,
            start=self.start,
        )


class ScenarioInstance(DomainModel):
    """This represents the reading version of scenario instance.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the scenario instance.
        data_record: The data record of the scenario instance node.
        aggregation: The aggregation field.
        country: The country field.
        instance: The instance field.
        market: The market field.
        price_area: The price area field.
        price_forecast: The price forecast field.
        scenario: The scenario field.
        start: The start field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("IntegrationTestsImmutable", "ScenarioInstance", "ee2b79fd98b5bb")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    aggregation: Optional[str] = None
    country: Optional[str] = None
    instance: Optional[datetime.datetime] = None
    market: Optional[str] = None
    price_area: Optional[str] = Field(None, alias="priceArea")
    price_forecast: Union[TimeSeries, str, None] = Field(None, alias="priceForecast")
    scenario: Optional[str] = None
    start: Optional[datetime.datetime] = None

    def as_write(self) -> ScenarioInstanceWrite:
        """Convert this read version of scenario instance to the writing version."""
        return ScenarioInstanceWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            aggregation=self.aggregation,
            country=self.country,
            instance=self.instance,
            market=self.market,
            price_area=self.price_area,
            price_forecast=(
                self.price_forecast.as_write()
                if isinstance(self.price_forecast, CogniteTimeSeries)
                else self.price_forecast
            ),
            scenario=self.scenario,
            start=self.start,
        )

    def as_apply(self) -> ScenarioInstanceWrite:
        """Convert this read version of scenario instance to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ScenarioInstanceWrite(DomainModelWrite):
    """This represents the writing version of scenario instance.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the scenario instance.
        data_record: The data record of the scenario instance node.
        aggregation: The aggregation field.
        country: The country field.
        instance: The instance field.
        market: The market field.
        price_area: The price area field.
        price_forecast: The price forecast field.
        scenario: The scenario field.
        start: The start field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("IntegrationTestsImmutable", "ScenarioInstance", "ee2b79fd98b5bb")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    aggregation: Optional[str] = None
    country: Optional[str] = None
    instance: Optional[datetime.datetime] = None
    market: Optional[str] = None
    price_area: Optional[str] = Field(None, alias="priceArea")
    price_forecast: Union[TimeSeriesWrite, str, None] = Field(None, alias="priceForecast")
    scenario: Optional[str] = None
    start: Optional[datetime.datetime] = None

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

        if self.aggregation is not None or write_none:
            properties["aggregation"] = self.aggregation

        if self.country is not None or write_none:
            properties["country"] = self.country

        if self.instance is not None or write_none:
            properties["instance"] = self.instance.isoformat(timespec="milliseconds") if self.instance else None

        if self.market is not None or write_none:
            properties["market"] = self.market

        if self.price_area is not None or write_none:
            properties["priceArea"] = self.price_area

        if self.price_forecast is not None or write_none:
            properties["priceForecast"] = (
                self.price_forecast
                if isinstance(self.price_forecast, str) or self.price_forecast is None
                else self.price_forecast.external_id
            )

        if self.scenario is not None or write_none:
            properties["scenario"] = self.scenario

        if self.start is not None or write_none:
            properties["start"] = self.start.isoformat(timespec="milliseconds") if self.start else None

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

        if isinstance(self.price_forecast, CogniteTimeSeriesWrite):
            resources.time_series.append(self.price_forecast)

        return resources


class ScenarioInstanceApply(ScenarioInstanceWrite):
    def __new__(cls, *args, **kwargs) -> ScenarioInstanceApply:
        warnings.warn(
            "ScenarioInstanceApply is deprecated and will be removed in v1.0. Use ScenarioInstanceWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "ScenarioInstance.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class ScenarioInstanceList(DomainModelList[ScenarioInstance]):
    """List of scenario instances in the read version."""

    _INSTANCE = ScenarioInstance

    def as_write(self) -> ScenarioInstanceWriteList:
        """Convert these read versions of scenario instance to the writing versions."""
        return ScenarioInstanceWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> ScenarioInstanceWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ScenarioInstanceWriteList(DomainModelWriteList[ScenarioInstanceWrite]):
    """List of scenario instances in the writing version."""

    _INSTANCE = ScenarioInstanceWrite


class ScenarioInstanceApplyList(ScenarioInstanceWriteList): ...


def _create_scenario_instance_filter(
    view_id: dm.ViewId,
    aggregation: str | list[str] | None = None,
    aggregation_prefix: str | None = None,
    country: str | list[str] | None = None,
    country_prefix: str | None = None,
    min_instance: datetime.datetime | None = None,
    max_instance: datetime.datetime | None = None,
    market: str | list[str] | None = None,
    market_prefix: str | None = None,
    price_area: str | list[str] | None = None,
    price_area_prefix: str | None = None,
    scenario: str | list[str] | None = None,
    scenario_prefix: str | None = None,
    min_start: datetime.datetime | None = None,
    max_start: datetime.datetime | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(aggregation, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("aggregation"), value=aggregation))
    if aggregation and isinstance(aggregation, list):
        filters.append(dm.filters.In(view_id.as_property_ref("aggregation"), values=aggregation))
    if aggregation_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("aggregation"), value=aggregation_prefix))
    if isinstance(country, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("country"), value=country))
    if country and isinstance(country, list):
        filters.append(dm.filters.In(view_id.as_property_ref("country"), values=country))
    if country_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("country"), value=country_prefix))
    if min_instance is not None or max_instance is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("instance"),
                gte=min_instance.isoformat(timespec="milliseconds") if min_instance else None,
                lte=max_instance.isoformat(timespec="milliseconds") if max_instance else None,
            )
        )
    if isinstance(market, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("market"), value=market))
    if market and isinstance(market, list):
        filters.append(dm.filters.In(view_id.as_property_ref("market"), values=market))
    if market_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("market"), value=market_prefix))
    if isinstance(price_area, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("priceArea"), value=price_area))
    if price_area and isinstance(price_area, list):
        filters.append(dm.filters.In(view_id.as_property_ref("priceArea"), values=price_area))
    if price_area_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("priceArea"), value=price_area_prefix))
    if isinstance(scenario, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("scenario"), value=scenario))
    if scenario and isinstance(scenario, list):
        filters.append(dm.filters.In(view_id.as_property_ref("scenario"), values=scenario))
    if scenario_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("scenario"), value=scenario_prefix))
    if min_start is not None or max_start is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("start"),
                gte=min_start.isoformat(timespec="milliseconds") if min_start else None,
                lte=max_start.isoformat(timespec="milliseconds") if max_start else None,
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


class _ScenarioInstanceQuery(NodeQueryCore[T_DomainModelList, ScenarioInstanceList]):
    _view_id = ScenarioInstance._view_id
    _result_cls = ScenarioInstance
    _result_list_cls_end = ScenarioInstanceList

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

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.aggregation = StringFilter(self, self._view_id.as_property_ref("aggregation"))
        self.country = StringFilter(self, self._view_id.as_property_ref("country"))
        self.instance = TimestampFilter(self, self._view_id.as_property_ref("instance"))
        self.market = StringFilter(self, self._view_id.as_property_ref("market"))
        self.price_area = StringFilter(self, self._view_id.as_property_ref("priceArea"))
        self.scenario = StringFilter(self, self._view_id.as_property_ref("scenario"))
        self.start = TimestampFilter(self, self._view_id.as_property_ref("start"))
        self._filter_classes.extend(
            [
                self.space,
                self.external_id,
                self.aggregation,
                self.country,
                self.instance,
                self.market,
                self.price_area,
                self.scenario,
                self.start,
            ]
        )

    def list_scenario_instance(self, limit: int = DEFAULT_QUERY_LIMIT) -> ScenarioInstanceList:
        return self._list(limit=limit)


class ScenarioInstanceQuery(_ScenarioInstanceQuery[ScenarioInstanceList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ScenarioInstanceList)
