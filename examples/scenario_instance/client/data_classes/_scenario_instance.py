from __future__ import annotations

import datetime
from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

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
    """This represent a read version of scenario instance.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the scenario instance.
        aggregation: The aggregation field.
        country: The country field.
        instance: The instance field.
        market: The market field.
        price_area: The price area field.
        price_forecast: The price forecast field.
        scenario: The scenario field.
        start: The start field.
        created_time: The created time of the scenario instance node.
        last_updated_time: The last updated time of the scenario instance node.
        deleted_time: If present, the deleted time of the scenario instance node.
        version: The version of the scenario instance node.
    """

    space: str = "IntegrationTestsImmutable"
    aggregation: Optional[str] = None
    country: Optional[str] = None
    instance: Optional[datetime.datetime] = None
    market: Optional[str] = None
    price_area: Optional[str] = Field(None, alias="priceArea")
    price_forecast: Optional[str] = Field(None, alias="priceForecast")
    scenario: Optional[str] = None
    start: Optional[datetime.datetime] = None

    def as_apply(self) -> ScenarioInstanceApply:
        """Convert this read version of scenario instance to a write version."""
        return ScenarioInstanceApply(
            space=self.space,
            external_id=self.external_id,
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
    """This represent a write version of scenario instance.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the scenario instance.
        aggregation: The aggregation field.
        country: The country field.
        instance: The instance field.
        market: The market field.
        price_area: The price area field.
        price_forecast: The price forecast field.
        scenario: The scenario field.
        start: The start field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "IntegrationTestsImmutable"
    aggregation: Optional[str] = None
    country: Optional[str] = None
    instance: Optional[datetime.datetime] = None
    market: Optional[str] = None
    price_area: Optional[str] = Field(None, alias="priceArea")
    price_forecast: Optional[str] = Field(None, alias="priceForecast")
    scenario: Optional[str] = None
    start: Optional[datetime.datetime] = None

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.aggregation is not None:
            properties["aggregation"] = self.aggregation
        if self.country is not None:
            properties["country"] = self.country
        if self.instance is not None:
            properties["instance"] = self.instance.isoformat()
        if self.market is not None:
            properties["market"] = self.market
        if self.price_area is not None:
            properties["priceArea"] = self.price_area
        if self.price_forecast is not None:
            properties["priceForecast"] = self.price_forecast
        if self.scenario is not None:
            properties["scenario"] = self.scenario
        if self.start is not None:
            properties["start"] = self.start.isoformat()
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("IntegrationTestsImmutable", "ScenarioInstance", "ee2b79fd98b5bb"),
                properties=properties,
            )
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[source],
            )
            nodes = [this_node]
        else:
            nodes = []

        edges = []
        cache.add(self.external_id)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class ScenarioInstanceList(TypeList[ScenarioInstance]):
    """List of scenario instances in read version."""

    _NODE = ScenarioInstance

    def as_apply(self) -> ScenarioInstanceApplyList:
        """Convert this read version of scenario instance to a write version."""
        return ScenarioInstanceApplyList([node.as_apply() for node in self.data])


class ScenarioInstanceApplyList(TypeApplyList[ScenarioInstanceApply]):
    """List of scenario instances in write version."""

    _NODE = ScenarioInstanceApply
