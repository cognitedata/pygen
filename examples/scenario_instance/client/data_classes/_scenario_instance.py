from __future__ import annotations

import datetime
from typing import ClassVar, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeList, TypeApplyList

__all__ = ["ScenarioInstance", "ScenarioInstanceApply", "ScenarioInstanceList", "ScenarioInstanceApplyList"]


class ScenarioInstance(DomainModel):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    aggregation: Optional[str] = None
    country: Optional[str] = None
    instance: Optional[datetime.datetime] = None
    market: Optional[str] = None
    price_area: Optional[str] = Field(None, alias="priceArea")
    price_forecast: Optional[str] = Field(None, alias="priceForecast")
    scenario: Optional[str] = None
    start: Optional[datetime.datetime] = None


class ScenarioInstanceApply(DomainModelApply):
    space: ClassVar[str] = "IntegrationTestsImmutable"
    aggregation: Optional[str] = None
    country: Optional[str] = None
    instance: Optional[datetime.datetime] = None
    market: Optional[str] = None
    price_area: Optional[str] = None
    price_forecast: Optional[str] = None
    scenario: Optional[str] = None
    start: Optional[datetime.datetime] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
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
                source=dm.ContainerId("IntegrationTestsImmutable", "ScenarioInstance"),
                properties=properties,
            )
            sources.append(source)
        if sources:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=sources,
            )
            nodes = [this_node]
        else:
            nodes = []

        edges = []
        cache.add(self.external_id)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class ScenarioInstanceList(TypeList[ScenarioInstance]):
    _NODE = ScenarioInstance


class ScenarioInstanceApplyList(TypeApplyList[ScenarioInstanceApply]):
    _NODE = ScenarioInstanceApply
