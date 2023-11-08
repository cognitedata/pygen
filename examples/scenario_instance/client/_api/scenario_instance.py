from __future__ import annotations

import datetime
import warnings
from typing import Sequence, overload, Literal

import pandas as pd
from cognite.client import CogniteClient
from cognite.client.data_classes import TimeSeriesList, DatapointsList, Datapoints, DatapointsArrayList
from cognite.client.data_classes.datapoints import Aggregate
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT, INSTANCE_QUERY_LIMIT
from scenario_instance.client.data_classes import (
    ScenarioInstance,
    ScenarioInstanceApply,
    ScenarioInstanceList,
    ScenarioInstanceApplyList,
    ScenarioInstanceFields,
    ScenarioInstanceTextFields,
    DomainModelApply,
)
from scenario_instance.client.data_classes._scenario_instance import _SCENARIOINSTANCE_PROPERTIES_BY_FIELD


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
        start: int | str | datetime.datetime | None = None,
        end: int | str | datetime.datetime | None = None,
        *,
        aggregates: Aggregate | list[Aggregate] | None = None,
        granularity: str | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
    ) -> DatapointsList:
        """`Retrieve datapoints for the `scenario_instance.price_forecast` timeseries.

        **Performance guide**:
            In order to retrieve millions of datapoints as efficiently as possible, here are a few guidelines:

            1. For best speed, and significantly lower memory usage, consider using ``retrieve_arrays(...)`` which uses ``numpy.ndarrays`` for data storage.
            2. Only unlimited queries with (``limit=None``) are fetched in parallel so specifying a large finite ``limit`` like 1 million, comes with severe performance penalty as data is fetched serially.
            3. Try to avoid specifying `start` and `end` to be very far from the actual data: If you have data from 2000 to 2015, don't set start=0 (1970).

        Args:
            start: Inclusive start. Default: 1970-01-01 UTC.
            end: Exclusive end. Default: "now"
            aggregates: Single aggregate or list of aggregates to retrieve. Default: None (raw datapoints returned)
            granularity The granularity to fetch aggregates at. e.g. '15s', '2h', '10d'. Default: None.
            target_unit: The unit_external_id of the data points returned. If the time series does not have a unit_external_id that can be converted to the target_unit, an error will be returned. Cannot be used with target_unit_system.
            target_unit_system: The unit system of the data points returned. Cannot be used with target_unit.
            limit (int | None): Maximum number of datapoints to return for each time series. Default: None (no limit)
            include_outside_points (bool): Whether to include outside points. Not allowed when fetching aggregates. Default: False

        Returns:
            A ``DatapointsList`` with the requested datapoints.

        Examples:

            In this example,
            we are using the time-ago format to get raw data for the 'my_price_forecast' from 2 weeks ago up until now::

                >>> from scenario_instance.client import ScenarioInstanceClient
                >>> client = ScenarioInstanceClient()
                >>> scenario_instance_datapoints = client.scenario_instance.price_forecast(external_id="my_price_forecast").retrieve(start="2w-ago)
        """
        external_ids = self._retrieve_timeseries_external_ids_with_extra()
        if external_ids:
            return self._client.time_series.data.retrieve(
                external_id=list(external_ids),
                start=start,
                end=end,
                aggregates=aggregates,
                granularity=granularity,
                target_unit=target_unit,
                target_unit_system=target_unit_system,
                limit=limit,
                include_outside_points=include_outside_points,
            )
        else:
            return DatapointsList([])

    def retrieve_arrays(
        self,
        start: int | str | datetime.datetime | None = None,
        end: int | str | datetime.datetime | None = None,
        *,
        aggregates: Aggregate | list[Aggregate] | None = None,
        granularity: str | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
    ) -> DatapointsArrayList:
        """`Retrieve numpy arrays for the `scenario_instance.price_forecast` timeseries.

        **Performance guide**:
            In order to retrieve millions of datapoints as efficiently as possible, here are a few guidelines:

            1. For best speed, and significantly lower memory usage, consider using ``retrieve_arrays(...)`` which uses ``numpy.ndarrays`` for data storage.
            2. Only unlimited queries with (``limit=None``) are fetched in parallel so specifying a large finite ``limit`` like 1 million, comes with severe performance penalty as data is fetched serially.
            3. Try to avoid specifying `start` and `end` to be very far from the actual data: If you have data from 2000 to 2015, don't set start=0 (1970).

        Args:
            start: Inclusive start. Default: 1970-01-01 UTC.
            end: Exclusive end. Default: "now"
            aggregates: Single aggregate or list of aggregates to retrieve. Default: None (raw datapoints returned)
            granularity The granularity to fetch aggregates at. e.g. '15s', '2h', '10d'. Default: None.
            target_unit: The unit_external_id of the data points returned. If the time series does not have a unit_external_id that can be converted to the target_unit, an error will be returned. Cannot be used with target_unit_system.
            target_unit_system: The unit system of the data points returned. Cannot be used with target_unit.
            limit (int | None): Maximum number of datapoints to return for each time series. Default: None (no limit)
            include_outside_points (bool): Whether to include outside points. Not allowed when fetching aggregates. Default: False

        Returns:
            A ``DatapointsArrayList`` with the requested datapoints.

        Examples:

            In this example,
            we are using the time-ago format to get raw data for the 'my_price_forecast' from 2 weeks ago up until now::

                >>> from scenario_instance.client import ScenarioInstanceClient
                >>> client = ScenarioInstanceClient()
                >>> scenario_instance_datapoints = client.scenario_instance.price_forecast(external_id="my_price_forecast").retrieve_array(start="2w-ago)
        """
        external_ids = self._retrieve_timeseries_external_ids_with_extra()
        if external_ids:
            return self._client.time_series.data.retrieve_arrays(
                external_id=list(external_ids),
                start=start,
                end=end,
                aggregates=aggregates,
                granularity=granularity,
                target_unit=target_unit,
                target_unit_system=target_unit_system,
                limit=limit,
                include_outside_points=include_outside_points,
            )
        else:
            return DatapointsArrayList([])

    def retrieve_dataframe(
        self,
        start: int | str | datetime.datetime | None = None,
        end: int | str | datetime.datetime | None = None,
        *,
        aggregates: Aggregate | list[Aggregate] | None = None,
        granularity: str | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
        uniform_index: bool = False,
        include_aggregate_name: bool = True,
        include_granularity_name: bool = False,
        column_names: ColumnNames | list[ColumnNames] = "priceForecast",
    ) -> pd.DataFrame:
        """`Retrieve DataFrames for the `scenario_instance.price_forecast` timeseries.

        **Performance guide**:
            In order to retrieve millions of datapoints as efficiently as possible, here are a few guidelines:

            1. For best speed, and significantly lower memory usage, consider using ``retrieve_arrays(...)`` which uses ``numpy.ndarrays`` for data storage.
            2. Only unlimited queries with (``limit=None``) are fetched in parallel so specifying a large finite ``limit`` like 1 million, comes with severe performance penalty as data is fetched serially.
            3. Try to avoid specifying `start` and `end` to be very far from the actual data: If you have data from 2000 to 2015, don't set start=0 (1970).

        Args:
            start: Inclusive start. Default: 1970-01-01 UTC.
            end: Exclusive end. Default: "now"
            aggregates: Single aggregate or list of aggregates to retrieve. Default: None (raw datapoints returned)
            granularity The granularity to fetch aggregates at. e.g. '15s', '2h', '10d'. Default: None.
            target_unit: The unit_external_id of the data points returned. If the time series does not have a unit_external_id that can be converted to the target_unit, an error will be returned. Cannot be used with target_unit_system.
            target_unit_system: The unit system of the data points returned. Cannot be used with target_unit.
            limit: Maximum number of datapoints to return for each time series. Default: None (no limit)
            include_outside_points: Whether to include outside points. Not allowed when fetching aggregates. Default: False
            uniform_index: If only querying aggregates AND a single granularity is used AND no limit is used, specifying `uniform_index=True` will return a dataframe with an equidistant datetime index from the earliest `start` to the latest `end` (missing values will be NaNs). If these requirements are not met, a ValueError is raised. Default: False
            include_aggregate_name: Include 'aggregate' in the column name, e.g. `my-ts|average`. Ignored for raw time series. Default: True
            include_granularity_name: Include 'granularity' in the column name, e.g. `my-ts|12h`. Added after 'aggregate' when present. Ignored for raw time series. Default: False
            column_names: Which property to use for column names. Defauts to priceForecast


        Returns:
            A ``DataFrame`` with the requested datapoints.

        Examples:

            In this example,
            we are using the time-ago format to get raw data for the 'my_price_forecast' from 2 weeks ago up until now::

                >>> from scenario_instance.client import ScenarioInstanceClient
                >>> client = ScenarioInstanceClient()
                >>> scenario_instance_datapoints = client.scenario_instance.price_forecast(external_id="my_price_forecast").retrieve_dataframe(start="2w-ago)
        """
        external_ids = self._retrieve_timeseries_external_ids_with_extra(column_names)
        if external_ids:
            df = self._client.time_series.data.retrieve_dataframe(
                external_id=list(external_ids),
                start=start,
                end=end,
                aggregates=aggregates,
                granularity=granularity,
                target_unit=target_unit,
                target_unit_system=target_unit_system,
                limit=limit,
                include_outside_points=include_outside_points,
                uniform_index=uniform_index,
                include_aggregate_name=include_aggregate_name,
                include_granularity_name=include_granularity_name,
            )
            is_aggregate = aggregates is not None
            return self._rename_columns(
                external_ids,
                df,
                column_names,
                is_aggregate and include_aggregate_name,
                is_aggregate and include_granularity_name,
            )
        else:
            return pd.DataFrame()

    def retrieve_dataframe_in_tz(
        self,
        start: datetime.datetime,
        end: datetime.datetime,
        *,
        aggregates: Aggregate | Sequence[Aggregate] | None = None,
        granularity: str | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        uniform_index: bool = False,
        include_aggregate_name: bool = True,
        include_granularity_name: bool = False,
        column_names: ColumnNames | list[ColumnNames] = "priceForecast",
    ) -> pd.DataFrame:
        """Retrieve DataFrames for the `scenario_instance.price_forecast` timeseries in Timezone.

        **Performance guide**:
            In order to retrieve millions of datapoints as efficiently as possible, here are a few guidelines:

            1. For best speed, and significantly lower memory usage, consider using ``retrieve_arrays(...)`` which uses ``numpy.ndarrays`` for data storage.
            2. Only unlimited queries with (``limit=None``) are fetched in parallel so specifying a large finite ``limit`` like 1 million, comes with severe performance penalty as data is fetched serially.
            3. Try to avoid specifying `start` and `end` to be very far from the actual data: If you have data from 2000 to 2015, don't set start=0 (1970).

        Args:
            start: Inclusive start.
            end: Exclusive end
            aggregates: Single aggregate or list of aggregates to retrieve. Default: None (raw datapoints returned)
            granularity The granularity to fetch aggregates at. e.g. '15s', '2h', '10d'. Default: None.
            target_unit: The unit_external_id of the data points returned. If the time series does not have a unit_external_id that can be converted to the target_unit, an error will be returned. Cannot be used with target_unit_system.
            target_unit_system: The unit system of the data points returned. Cannot be used with target_unit.
            limit: Maximum number of datapoints to return for each time series. Default: None (no limit)
            include_outside_points: Whether to include outside points. Not allowed when fetching aggregates. Default: False
            uniform_index: If only querying aggregates AND a single granularity is used AND no limit is used, specifying `uniform_index=True` will return a dataframe with an equidistant datetime index from the earliest `start` to the latest `end` (missing values will be NaNs). If these requirements are not met, a ValueError is raised. Default: False
            include_aggregate_name: Include 'aggregate' in the column name, e.g. `my-ts|average`. Ignored for raw time series. Default: True
            include_granularity_name: Include 'granularity' in the column name, e.g. `my-ts|12h`. Added after 'aggregate' when present. Ignored for raw time series. Default: False
            column_names: Which property to use for column names. Defauts to priceForecast


        Returns:
            A ``DataFrame`` with the requested datapoints.

        Examples:

            In this example,
            get weekly aggregates for the 'my_price_forecast' for the first month of 2023 in Oslo time:

                >>> from scenario_instance.client import ScenarioInstanceClient
                >>> from datetime import datetime, timezone
                >>> client = ScenarioInstanceClient()
                >>> scenario_instance_datapoints = client.scenario_instance.price_forecast(
                ...     external_id="my_price_forecast").retrieve_dataframe_in_timezone(
                ...         datetime(2023, 1, 1, tzinfo=ZoneInfo("Europe/Oslo")),
                ...         datetime(2023, 1, 2, tzinfo=ZoneInfo("Europe/Oslo")),
                ...         aggregates="average",
                ...         granularity="1week",
                ...     )
        """
        external_ids = self._retrieve_timeseries_external_ids_with_extra(column_names)
        if external_ids:
            df = self._client.time_series.data.retrieve_dataframe_in_tz(
                external_id=list(external_ids),
                start=start,
                end=end,
                aggregates=aggregates,
                granularity=granularity,
                target_unit=target_unit,
                target_unit_system=target_unit_system,
                uniform_index=uniform_index,
                include_aggregate_name=include_aggregate_name,
                include_granularity_name=include_granularity_name,
            )
            is_aggregate = aggregates is not None
            return self._rename_columns(
                external_ids,
                df,
                column_names,
                is_aggregate and include_aggregate_name,
                is_aggregate and include_granularity_name,
            )
        else:
            return pd.DataFrame()

    def retrieve_latest(
        self,
        before: None | int | str | datetime.datetime = None,
    ) -> Datapoints | DatapointsList | None:
        external_ids = self._retrieve_timeseries_external_ids_with_extra()
        if external_ids:
            return self._client.time_series.data.retrieve_latest(
                external_id=list(external_ids),
                before=before,
            )
        else:
            return None

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
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ScenarioInstancePriceForecastQuery:
        """Query timeseries `scenario_instance.price_forecast`

        Args:
            aggregation: The aggregation to filter on.
            aggregation_prefix: The prefix of the aggregation to filter on.
            country: The country to filter on.
            country_prefix: The prefix of the country to filter on.
            min_instance: The minimum value of the instance to filter on.
            max_instance: The maximum value of the instance to filter on.
            market: The market to filter on.
            market_prefix: The prefix of the market to filter on.
            price_area: The price area to filter on.
            price_area_prefix: The prefix of the price area to filter on.
            scenario: The scenario to filter on.
            scenario_prefix: The prefix of the scenario to filter on.
            min_start: The minimum value of the start to filter on.
            max_start: The maximum value of the start to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of scenario instances to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query object that can be used to retrieve datapoins for the  scenario_instance.price_forecast timeseries
            selected in this method.

        Examples:

            Retrieve all data for 5 scenario_instance.price_forecast timeseries:

                >>> from scenario_instance.client import ScenarioInstanceClient
                >>> client = ScenarioInstanceClient()
                >>> scenario_instances = client.scenario_instance.price_forecast(limit=5).retrieve()

        """
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
            space,
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
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> TimeSeriesList:
        """List timeseries `scenario_instance.price_forecast`

        Args:
            aggregation: The aggregation to filter on.
            aggregation_prefix: The prefix of the aggregation to filter on.
            country: The country to filter on.
            country_prefix: The prefix of the country to filter on.
            min_instance: The minimum value of the instance to filter on.
            max_instance: The maximum value of the instance to filter on.
            market: The market to filter on.
            market_prefix: The prefix of the market to filter on.
            price_area: The price area to filter on.
            price_area_prefix: The prefix of the price area to filter on.
            scenario: The scenario to filter on.
            scenario_prefix: The prefix of the scenario to filter on.
            min_start: The minimum value of the start to filter on.
            max_start: The maximum value of the start to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of scenario instances to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of Timeseries scenario_instance.price_forecast.

        Examples:

            List scenario_instance.price_forecast and limit to 5:

                >>> from scenario_instance.client import ScenarioInstanceClient
                >>> client = ScenarioInstanceClient()
                >>> scenario_instances = client.scenario_instance.price_forecast.list(limit=5)

        """
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
            space,
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
            node.properties[view_id]["priceForecast"]: [node.properties[view_id].get(prop, "") for prop in extra_list]
            for node in result.data["nodes"].data
        }
        total_retrieved += len(batch_external_ids)
        external_ids.update(batch_external_ids)
        cursor = result.cursors["nodes"]
        if total_retrieved >= limit or cursor is None:
            break
    return external_ids


class ScenarioInstanceAPI(TypeAPI[ScenarioInstance, ScenarioInstanceApply, ScenarioInstanceList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[ScenarioInstanceApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=ScenarioInstance,
            class_apply_type=ScenarioInstanceApply,
            class_list=ScenarioInstanceList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class
        self.price_forecast = ScenarioInstancePriceForecastAPI(client, view_id)

    def apply(
        self, scenario_instance: ScenarioInstanceApply | Sequence[ScenarioInstanceApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) scenario instances.

        Args:
            scenario_instance: Scenario instance or sequence of scenario instances to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new scenario_instance:

                >>> from scenario_instance.client import ScenarioInstanceClient
                >>> from scenario_instance.client.data_classes import ScenarioInstanceApply
                >>> client = ScenarioInstanceClient()
                >>> scenario_instance = ScenarioInstanceApply(external_id="my_scenario_instance", ...)
                >>> result = client.scenario_instance.apply(scenario_instance)

        """
        if isinstance(scenario_instance, ScenarioInstanceApply):
            instances = scenario_instance.to_instances_apply(self._view_by_write_class)
        else:
            instances = ScenarioInstanceApplyList(scenario_instance).to_instances_apply(self._view_by_write_class)
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more scenario instance.

        Args:
            external_id: External id of the scenario instance to delete.
            space: The space where all the scenario instance are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete scenario_instance by id:

                >>> from scenario_instance.client import ScenarioInstanceClient
                >>> client = ScenarioInstanceClient()
                >>> client.scenario_instance.delete("my_scenario_instance")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> ScenarioInstance:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> ScenarioInstanceList:
        ...

    def retrieve(
        self, external_id: str | Sequence[str], space: str = "IntegrationTestsImmutable"
    ) -> ScenarioInstance | ScenarioInstanceList:
        """Retrieve one or more scenario instances by id(s).

        Args:
            external_id: External id or list of external ids of the scenario instances.
            space: The space where all the scenario instances are located.

        Returns:
            The requested scenario instances.

        Examples:

            Retrieve scenario_instance by id:

                >>> from scenario_instance.client import ScenarioInstanceClient
                >>> client = ScenarioInstanceClient()
                >>> scenario_instance = client.scenario_instance.retrieve("my_scenario_instance")

        """
        if isinstance(external_id, str):
            return self._retrieve((space, external_id))
        else:
            return self._retrieve([(space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: ScenarioInstanceTextFields | Sequence[ScenarioInstanceTextFields] | None = None,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ScenarioInstanceList:
        """Search scenario instances

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            aggregation: The aggregation to filter on.
            aggregation_prefix: The prefix of the aggregation to filter on.
            country: The country to filter on.
            country_prefix: The prefix of the country to filter on.
            min_instance: The minimum value of the instance to filter on.
            max_instance: The maximum value of the instance to filter on.
            market: The market to filter on.
            market_prefix: The prefix of the market to filter on.
            price_area: The price area to filter on.
            price_area_prefix: The prefix of the price area to filter on.
            scenario: The scenario to filter on.
            scenario_prefix: The prefix of the scenario to filter on.
            min_start: The minimum value of the start to filter on.
            max_start: The maximum value of the start to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of scenario instances to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results scenario instances matching the query.

        Examples:

           Search for 'my_scenario_instance' in all text properties:

                >>> from scenario_instance.client import ScenarioInstanceClient
                >>> client = ScenarioInstanceClient()
                >>> scenario_instances = client.scenario_instance.search('my_scenario_instance')

        """
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
            space,
            filter,
        )
        return self._search(self._view_id, query, _SCENARIOINSTANCE_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: ScenarioInstanceFields | Sequence[ScenarioInstanceFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: ScenarioInstanceTextFields | Sequence[ScenarioInstanceTextFields] | None = None,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: ScenarioInstanceFields | Sequence[ScenarioInstanceFields] | None = None,
        group_by: ScenarioInstanceFields | Sequence[ScenarioInstanceFields] = None,
        query: str | None = None,
        search_properties: ScenarioInstanceTextFields | Sequence[ScenarioInstanceTextFields] | None = None,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList:
        ...

    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: ScenarioInstanceFields | Sequence[ScenarioInstanceFields] | None = None,
        group_by: ScenarioInstanceFields | Sequence[ScenarioInstanceFields] | None = None,
        query: str | None = None,
        search_property: ScenarioInstanceTextFields | Sequence[ScenarioInstanceTextFields] | None = None,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across scenario instances

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            aggregation: The aggregation to filter on.
            aggregation_prefix: The prefix of the aggregation to filter on.
            country: The country to filter on.
            country_prefix: The prefix of the country to filter on.
            min_instance: The minimum value of the instance to filter on.
            max_instance: The maximum value of the instance to filter on.
            market: The market to filter on.
            market_prefix: The prefix of the market to filter on.
            price_area: The price area to filter on.
            price_area_prefix: The prefix of the price area to filter on.
            scenario: The scenario to filter on.
            scenario_prefix: The prefix of the scenario to filter on.
            min_start: The minimum value of the start to filter on.
            max_start: The maximum value of the start to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of scenario instances to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count scenario instances in space `my_space`:

                >>> from scenario_instance.client import ScenarioInstanceClient
                >>> client = ScenarioInstanceClient()
                >>> result = client.scenario_instance.aggregate("count", space="my_space")

        """

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
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _SCENARIOINSTANCE_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: ScenarioInstanceFields,
        interval: float,
        query: str | None = None,
        search_property: ScenarioInstanceTextFields | Sequence[ScenarioInstanceTextFields] | None = None,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for scenario instances

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            aggregation: The aggregation to filter on.
            aggregation_prefix: The prefix of the aggregation to filter on.
            country: The country to filter on.
            country_prefix: The prefix of the country to filter on.
            min_instance: The minimum value of the instance to filter on.
            max_instance: The maximum value of the instance to filter on.
            market: The market to filter on.
            market_prefix: The prefix of the market to filter on.
            price_area: The price area to filter on.
            price_area_prefix: The prefix of the price area to filter on.
            scenario: The scenario to filter on.
            scenario_prefix: The prefix of the scenario to filter on.
            min_start: The minimum value of the start to filter on.
            max_start: The maximum value of the start to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of scenario instances to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
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
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _SCENARIOINSTANCE_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
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
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ScenarioInstanceList:
        """List/filter scenario instances

        Args:
            aggregation: The aggregation to filter on.
            aggregation_prefix: The prefix of the aggregation to filter on.
            country: The country to filter on.
            country_prefix: The prefix of the country to filter on.
            min_instance: The minimum value of the instance to filter on.
            max_instance: The maximum value of the instance to filter on.
            market: The market to filter on.
            market_prefix: The prefix of the market to filter on.
            price_area: The price area to filter on.
            price_area_prefix: The prefix of the price area to filter on.
            scenario: The scenario to filter on.
            scenario_prefix: The prefix of the scenario to filter on.
            min_start: The minimum value of the start to filter on.
            max_start: The maximum value of the start to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of scenario instances to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested scenario instances

        Examples:

            List scenario instances and limit to 5:

                >>> from scenario_instance.client import ScenarioInstanceClient
                >>> client = ScenarioInstanceClient()
                >>> scenario_instances = client.scenario_instance.list(limit=5)

        """
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
            space,
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
    space: str | list[str] | None = None,
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
                gte=min_instance.isoformat(timespec="milliseconds") if min_instance else None,
                lte=max_instance.isoformat(timespec="milliseconds") if max_instance else None,
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
                gte=min_start.isoformat(timespec="milliseconds") if min_start else None,
                lte=max_start.isoformat(timespec="milliseconds") if max_start else None,
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
