from __future__ import annotations

import datetime
from typing import Any, Literal, Optional, Union

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


__all__ = [
    "ScenarioInstance",
    "ScenarioInstanceApply",
    "ScenarioInstanceList",
    "ScenarioInstanceApplyList",
    "ScenarioInstanceFields",
    "ScenarioInstanceTextFields",
]


ScenarioInstanceTextFields = Literal["aggregation", "country", "market", "price_area", "price_forecast", "scenario"]
ScenarioInstanceFields = Literal[
    "aggregation", "country", "instance", "market", "price_area", "price_forecast", "scenario", "start"
]

_SCENARIOINSTANCE_PROPERTIES_BY_FIELD = {
    "aggregation": "aggregation",
    "country": "country",
    "instance": "instance",
    "market": "market",
    "price_area": "priceArea",
    "price_forecast": "priceForecast",
    "scenario": "scenario",
    "start": "start",
}


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

    def as_apply(self) -> ScenarioInstanceApply:
        """Convert this read version of scenario instance to the writing version."""
        return ScenarioInstanceApply(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            aggregation=self.aggregation,
            country=self.country,
            instance=self.instance,
            market=self.market,
            price_area=self.price_area,
            price_forecast=self.price_forecast,
            scenario=self.scenario,
            start=self.start,
        )


class ScenarioInstanceApply(DomainModelApply):
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

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(
            ScenarioInstance, dm.ViewId("IntegrationTestsImmutable", "ScenarioInstance", "ee2b79fd98b5bb")
        )

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
            if isinstance(self.price_forecast, str) or self.price_forecast is None:
                properties["priceForecast"] = self.price_forecast
            else:
                properties["priceForecast"] = self.price_forecast.external_id

        if self.scenario is not None or write_none:
            properties["scenario"] = self.scenario

        if self.start is not None or write_none:
            properties["start"] = self.start.isoformat(timespec="milliseconds") if self.start else None

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

        if isinstance(self.price_forecast, TimeSeries):
            resources.time_series.append(self.price_forecast)

        return resources


class ScenarioInstanceList(DomainModelList[ScenarioInstance]):
    """List of scenario instances in the read version."""

    _INSTANCE = ScenarioInstance

    def as_apply(self) -> ScenarioInstanceApplyList:
        """Convert these read versions of scenario instance to the writing versions."""
        return ScenarioInstanceApplyList([node.as_apply() for node in self.data])


class ScenarioInstanceApplyList(DomainModelApplyList[ScenarioInstanceApply]):
    """List of scenario instances in the writing version."""

    _INSTANCE = ScenarioInstanceApply


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
    filters = []
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
