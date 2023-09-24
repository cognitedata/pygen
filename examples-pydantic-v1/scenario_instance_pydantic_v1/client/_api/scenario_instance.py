from __future__ import annotations

import datetime
import warnings
from typing import Sequence, overload, Literal

import pandas as pd
from cognite.client import CogniteClient
from cognite.client.data_classes import TimeSeriesList, DatapointsList, Datapoints, DatapointsArrayList
from cognite.client.data_classes.datapoints import Aggregate
from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, TypeAPI, INSTANCE_QUERY_LIMIT
from scenario_instance_pydantic_v1.client.data_classes import (
    ScenarioInstance,
    ScenarioInstanceApply,
    ScenarioInstanceList,
    ScenarioInstanceApplyList,
)

ColumnNames = Literal["aggregation", "country", "instance", "market", "priceArea", "priceForecast", "scenario", "start"]


class ScenarioInstancePriceForecastQuery:
    def __init__(
        self,
        client: CogniteClient,
        view_id: dm.ViewId,
        timeseries_limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ):
        self._client = client
        self._view_id = view_id
        self._timeseries_limit = timeseries_limit
        self._filter = filter

    def retrieve(
        self,
        start: int | str | datetime | None = None,
        end: int | str | datetime | None = None,
        *,
        aggregates: Aggregate | list[Aggregate] | None = None,
        granularity: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
    ) -> DatapointsList:
        external_ids = self._retrieve_timeseries_external_ids_with_extra()
        if external_ids:
            return self._client.time_series.data.retrieve(
                external_id=list(external_ids),
                start=start,
                end=end,
                aggregates=aggregates,
                granularity=granularity,
                limit=limit,
                include_outside_points=include_outside_points,
            )
        else:
            return DatapointsList([])

    def retrieve_arrays(
        self,
        start: int | str | datetime | None = None,
        end: int | str | datetime | None = None,
        *,
        aggregates: Aggregate | list[Aggregate] | None = None,
        granularity: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
    ) -> DatapointsArrayList:
        external_ids = self._retrieve_timeseries_external_ids_with_extra()
        if external_ids:
            return self._client.time_series.data.retrieve_arrays(
                external_id=list(external_ids),
                start=start,
                end=end,
                aggregates=aggregates,
                granularity=granularity,
                limit=limit,
                include_outside_points=include_outside_points,
            )
        else:
            return DatapointsArrayList([])

    def retrieve_dataframe(
        self,
        start: int | str | datetime | None = None,
        end: int | str | datetime | None = None,
        *,
        aggregates: Aggregate | list[Aggregate] | None = None,
        granularity: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
        uniform_index: bool = False,
        include_aggregate_name: bool = True,
        include_granularity_name: bool = False,
        column_names: ColumnNames | list[ColumnNames] = "priceForecast",
    ) -> pd.DataFrame:
        external_ids = self._retrieve_timeseries_external_ids_with_extra(column_names)
        if external_ids:
            df = self._client.time_series.data.retrieve_dataframe(
                external_id=list(external_ids),
                start=start,
                end=end,
                aggregates=aggregates,
                granularity=granularity,
                limit=limit,
                include_outside_points=include_outside_points,
                uniform_index=uniform_index,
                include_aggregate_name=include_aggregate_name,
                include_granularity_name=include_granularity_name,
            )
            return self._rename_columns(
                external_ids,
                df,
                column_names,
                include_aggregate_name,
                include_granularity_name,
            )
        else:
            return pd.DataFrame()

    def retrieve_dataframe_in_tz(
        self,
        start: datetime,
        end: datetime,
        *,
        aggregates: Aggregate | Sequence[Aggregate] | None = None,
        granularity: str | None = None,
        uniform_index: bool = False,
        include_aggregate_name: bool = True,
        include_granularity_name: bool = False,
        column_names: ColumnNames | list[ColumnNames] = "priceForecast",
    ) -> pd.DataFrame:
        external_ids = self._retrieve_timeseries_external_ids_with_extra(column_names)
        if external_ids:
            df = self._client.time_series.data.retrieve_dataframe_in_tz(
                external_id=list(external_ids),
                start=start,
                end=end,
                aggregates=aggregates,
                granularity=granularity,
                uniform_index=uniform_index,
                include_aggregate_name=include_aggregate_name,
                include_granularity_name=include_granularity_name,
            )
            return self._rename_columns(
                external_ids,
                df,
                column_names,
                include_aggregate_name,
                include_granularity_name,
            )
        else:
            return pd.DataFrame()

    def retrieve_latest(
        self,
        before: None | int | str | datetime = None,
    ) -> Datapoints | DatapointsList | None:
        external_ids = self._retrieve_timeseries_external_ids_with_extra()
        if external_ids:
            return self._client.time_series.data.retrieve_latest(
                external_id=list(external_ids),
                before=before,
            )
        else:
            return None

    def plot(
        self,
        start: int | str | datetime | None = None,
        end: int | str | datetime | None = None,
        *,
        aggregates: Aggregate | Sequence[Aggregate] | None = None,
        granularity: str | None = None,
        uniform_index: bool = False,
        include_aggregate_name: bool = True,
        include_granularity_name: bool = False,
        column_names: ColumnNames | list[ColumnNames] = "priceForecast",
        warning: bool = True,
        **kwargs,
    ) -> None:
        if warning:
            warnings.warn(
                "This methods if an experiment and might be removed in the future without notice.", stacklevel=2
            )
        if all(isinstance(time, datetime.datetime) and time.tzinfo is not None for time in [start, end]):
            df = self.retrieve_dataframe_in_tz(
                start=start,
                end=end,
                aggregates=aggregates,
                granularity=granularity,
                uniform_index=uniform_index,
                include_aggregate_name=include_aggregate_name,
                include_granularity_name=include_granularity_name,
                column_names=column_names,
            )
        else:
            df = self.retrieve_dataframe(
                start=start,
                end=end,
                aggregates=aggregates,
                granularity=granularity,
                uniform_index=uniform_index,
                include_aggregate_name=include_aggregate_name,
                include_granularity_name=include_granularity_name,
                column_names=column_names,
            )
        df.plot(**kwargs)

    def _retrieve_timeseries_external_ids_with_extra(
        self, extra_properties: ColumnNames | list[ColumnNames] = "priceForecast"
    ) -> dict[str, list[str]]:
        return _retrieve_timeseries_external_ids_with_extra_price_forecast(
            self._client,
            self._view_id,
            self._filter,
            self._timeseries_limit,
            extra_properties,
        )

    @staticmethod
    def _rename_columns(
        external_ids: dict[str, list[str]],
        df: pd.DataFrame,
        column_names: ColumnNames | list[ColumnNames],
        include_aggregate_name: bool,
        include_granularity_name: bool,
    ) -> pd.DataFrame:
        if isinstance(column_names, str) and column_names == "priceForecast":
            return df
        splits = sum(included for included in [include_aggregate_name, include_granularity_name])
        if splits == 0:
            df.columns = ["-".join(external_ids[external_id]) for external_id in df.columns]
        else:
            column_parts = (col.rsplit("|", maxsplit=splits) for col in df.columns)
            df.columns = [
                "-".join(external_ids[external_id]) + "|" + "|".join(parts) for external_id, *parts in column_parts
            ]
        return df


class ScenarioInstancePriceForecastAPI:
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        self._client = client
        self._view_id = view_id

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
        filter_ = _create_filter(
            self._view_id,
            aggregation,
            aggregation_prefix,
            country,
            country_prefix,
            min_instance,
            max_instance,
            market,
            market_prefix,
            price_area,
            price_area_prefix,
            scenario,
            scenario_prefix,
            min_start,
            max_start,
            external_id_prefix,
            filter,
        )

        return ScenarioInstancePriceForecastQuery(
            client=self._client,
            view_id=self._view_id,
            timeseries_limit=limit,
            filter=filter_,
        )

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
        filter_ = _create_filter(
            self._view_id,
            aggregation,
            aggregation_prefix,
            country,
            country_prefix,
            min_instance,
            max_instance,
            market,
            market_prefix,
            price_area,
            price_area_prefix,
            scenario,
            scenario_prefix,
            min_start,
            max_start,
            external_id_prefix,
            filter,
        )
        external_ids = _retrieve_timeseries_external_ids_with_extra_price_forecast(
            self._client, self._view_id, filter_, limit
        )
        if external_ids:
            return self._client.time_series.retrieve_multiple(external_ids=list(external_ids))
        else:
            return TimeSeriesList([])


def _retrieve_timeseries_external_ids_with_extra_price_forecast(
    client: CogniteClient,
    view_id: dm.ViewId,
    filter_: dm.Filter | None,
    limit: int,
    extra_properties: ColumnNames | list[ColumnNames] = "priceForecast",
) -> dict[str, list[str]]:
    properties = ["priceForecast"]
    if extra_properties == "priceForecast":
        ...
    elif isinstance(extra_properties, str) and extra_properties != "priceForecast":
        properties.append(extra_properties)
    elif isinstance(extra_properties, list):
        properties.extend([prop for prop in extra_properties if prop != "priceForecast"])
    else:
        raise ValueError(f"Invalid value for extra_properties: {extra_properties}")

    if isinstance(extra_properties, str):
        extra_list = [extra_properties]
    else:
        extra_list = extra_properties
    has_data = dm.filters.HasData([dm.ContainerId("IntegrationTestsImmutable", "ScenarioInstance")], [view_id])
    filter_ = dm.filters.And(filter_, has_data) if filter_ else has_data

    cursor = None
    external_ids: dict[str, list[str]] = {}
    total_retrieved = 0
    while True:
        query_limit = min(INSTANCE_QUERY_LIMIT, limit - total_retrieved)
        selected_nodes = dm.query.NodeResultSetExpression(filter=filter_, limit=query_limit)
        query = dm.query.Query(
            with_={
                "nodes": selected_nodes,
            },
            select={
                "nodes": dm.query.Select(
                    [dm.query.SourceSelector(view_id, properties)],
                )
            },
            cursors={"nodes": cursor},
        )
        result = client.data_modeling.instances.query(query)
        batch_external_ids = {
            node.properties[view_id]["priceForecast"]: [node.properties[view_id][prop] for prop in extra_list]
            for node in result.data["nodes"].data
        }
        total_retrieved += len(batch_external_ids)
        external_ids.update(batch_external_ids)
        cursor = result.cursors["nodes"]
        if total_retrieved >= limit or cursor is None:
            break
    return external_ids


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
        self.price_forecast = ScenarioInstancePriceForecastAPI(client, view_id)

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
        filter_ = _create_filter(
            self.view_id,
            aggregation,
            aggregation_prefix,
            country,
            country_prefix,
            min_instance,
            max_instance,
            market,
            market_prefix,
            price_area,
            price_area_prefix,
            scenario,
            scenario_prefix,
            min_start,
            max_start,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
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
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if aggregation and isinstance(aggregation, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("aggregation"), value=aggregation))
    if aggregation and isinstance(aggregation, list):
        filters.append(dm.filters.In(view_id.as_property_ref("aggregation"), values=aggregation))
    if aggregation_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("aggregation"), value=aggregation_prefix))
    if country and isinstance(country, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("country"), value=country))
    if country and isinstance(country, list):
        filters.append(dm.filters.In(view_id.as_property_ref("country"), values=country))
    if country_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("country"), value=country_prefix))
    if min_instance or max_instance:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("instance"),
                gte=min_instance.isoformat() if min_instance else None,
                lte=max_instance.isoformat() if max_instance else None,
            )
        )
    if market and isinstance(market, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("market"), value=market))
    if market and isinstance(market, list):
        filters.append(dm.filters.In(view_id.as_property_ref("market"), values=market))
    if market_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("market"), value=market_prefix))
    if price_area and isinstance(price_area, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("priceArea"), value=price_area))
    if price_area and isinstance(price_area, list):
        filters.append(dm.filters.In(view_id.as_property_ref("priceArea"), values=price_area))
    if price_area_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("priceArea"), value=price_area_prefix))
    if scenario and isinstance(scenario, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("scenario"), value=scenario))
    if scenario and isinstance(scenario, list):
        filters.append(dm.filters.In(view_id.as_property_ref("scenario"), values=scenario))
    if scenario_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("scenario"), value=scenario_prefix))
    if min_start or max_start:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("start"),
                gte=min_start.isoformat() if min_start else None,
                lte=max_start.isoformat() if max_start else None,
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
