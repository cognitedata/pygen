from __future__ import annotations

import datetime
from typing import Sequence, overload

import pandas as pd
from cognite.client import CogniteClient
from cognite.client.data_classes import TimeSeriesList, DatapointsList, Datapoints, DatapointsArray, DatapointsArrayList
from cognite.client.data_classes.datapoints import Aggregate
from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, TypeAPI
from scenario_instance.client.data_classes import (
    ScenarioInstance,
    ScenarioInstanceApply,
    ScenarioInstanceList,
    ScenarioInstanceApplyList,
)


class ScenarioInstancePriceForecastQuery:
    def __init__(
        self,
        client: CogniteClient,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ):
        self._client = client
        raise NotImplementedError()

    def retrieve(
        self,
        *,
        start: int | str | datetime | None = None,
        end: int | str | datetime | None = None,
        aggregates: Aggregate | list[Aggregate] | None = None,
        granularity: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
        ignore_unknown_ids: bool = False,
    ) -> Datapoints | DatapointsList | None:
        raise NotImplementedError()

    def retrieve_arrays(
        self,
        *,
        start: int | str | datetime | None = None,
        end: int | str | datetime | None = None,
        aggregates: Aggregate | list[Aggregate] | None = None,
        granularity: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
        ignore_unknown_ids: bool = False,
    ) -> DatapointsArray | DatapointsArrayList | None:
        raise NotImplementedError()

    def retrieve_dataframe(
        self,
        *,
        start: int | str | datetime | None = None,
        end: int | str | datetime | None = None,
        aggregates: Aggregate | list[Aggregate] | None = None,
        granularity: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
        uniform_index: bool = False,
        include_aggregate_name: bool = True,
        include_granularity_name: bool = False,
    ) -> pd.DataFrame:
        raise NotImplementedError()

    def retrieve_dataframe_in_tz(
        self,
        *,
        start: datetime,
        end: datetime,
        aggregates: Aggregate | Sequence[Aggregate] | None = None,
        granularity: str | None = None,
        uniform_index: bool = False,
        include_aggregate_name: bool = True,
        include_granularity_name: bool = False,
    ) -> pd.DataFrame:
        raise NotImplementedError()

    def retrieve_latest(
        self,
        before: None | int | str | datetime = None,
    ) -> Datapoints | DatapointsList | None:
        raise NotImplementedError()

    def plot(
        self,
        start: int | str | datetime | None = None,
        end: int | str | datetime | None = None,
        aggregates: Aggregate | Sequence[Aggregate] | None = None,
        granularity: str | None = None,
        ignore_unknown_ids: bool = False,
        uniform_index: bool = False,
        include_aggregate_name: bool = True,
        include_granularity_name: bool = False,
    ) -> None:
        raise NotImplementedError()


class ScenarioInstancePriceForecastAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def __call__(
        self,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ScenarioInstancePriceForecastQuery:
        raise NotImplementedError()

    def list(
        self,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> TimeSeriesList:
        raise NotImplementedError()


class ScenarioInstanceAPI(TypeAPI[ScenarioInstance, ScenarioInstanceApply, ScenarioInstanceList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=ScenarioInstance,
            class_apply_type=ScenarioInstanceApply,
            class_list=ScenarioInstanceList,
        )
        self.view_id = view_id
        self.price_forecast = ScenarioInstancePriceForecastAPI(client)

    def apply(
        self, scenario_instance: ScenarioInstanceApply | Sequence[ScenarioInstanceApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(scenario_instance, ScenarioInstanceApply):
            instances = scenario_instance.to_instances_apply()
        else:
            instances = ScenarioInstanceApplyList(scenario_instance).to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(ScenarioInstanceApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(ScenarioInstanceApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> ScenarioInstance:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> ScenarioInstanceList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> ScenarioInstance | ScenarioInstanceList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(
        self,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ScenarioInstanceList:
        filters = []
        if aggregation and isinstance(aggregation, str):
            filters.append(dm.filters.Equals(self.view_id.as_property_ref("aggregation"), value=aggregation))
        if aggregation and isinstance(aggregation, list):
            filters.append(dm.filters.In(self.view_id.as_property_ref("aggregation"), values=aggregation))
        if aggregation_prefix:
            filters.append(dm.filters.Prefix(self.view_id.as_property_ref("aggregation"), value=aggregation_prefix))
        if country and isinstance(country, str):
            filters.append(dm.filters.Equals(self.view_id.as_property_ref("country"), value=country))
        if country and isinstance(country, list):
            filters.append(dm.filters.In(self.view_id.as_property_ref("country"), values=country))
        if country_prefix:
            filters.append(dm.filters.Prefix(self.view_id.as_property_ref("country"), value=country_prefix))
        if min_instance or max_instance:
            filters.append(
                dm.filters.Range(self.view_id.as_property_ref("instance"), gte=min_instance, lte=max_instance)
            )
        if market and isinstance(market, str):
            filters.append(dm.filters.Equals(self.view_id.as_property_ref("market"), value=market))
        if market and isinstance(market, list):
            filters.append(dm.filters.In(self.view_id.as_property_ref("market"), values=market))
        if market_prefix:
            filters.append(dm.filters.Prefix(self.view_id.as_property_ref("market"), value=market_prefix))
        if price_area and isinstance(price_area, str):
            filters.append(dm.filters.Equals(self.view_id.as_property_ref("priceArea"), value=price_area))
        if price_area and isinstance(price_area, list):
            filters.append(dm.filters.In(self.view_id.as_property_ref("priceArea"), values=price_area))
        if price_area_prefix:
            filters.append(dm.filters.Prefix(self.view_id.as_property_ref("priceArea"), value=price_area_prefix))
        if scenario and isinstance(scenario, str):
            filters.append(dm.filters.Equals(self.view_id.as_property_ref("scenario"), value=scenario))
        if scenario and isinstance(scenario, list):
            filters.append(dm.filters.In(self.view_id.as_property_ref("scenario"), values=scenario))
        if scenario_prefix:
            filters.append(dm.filters.Prefix(self.view_id.as_property_ref("scenario"), value=scenario_prefix))
        if min_start or max_start:
            filters.append(dm.filters.Range(self.view_id.as_property_ref("start"), gte=min_start, lte=max_start))
        if external_id_prefix:
            filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
        if filter:
            filters.append(filter)

        return self._list(limit=limit, filter=dm.filters.And(*filters) if filters else None)
