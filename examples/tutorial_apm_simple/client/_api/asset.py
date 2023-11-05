from __future__ import annotations

import datetime
import warnings
from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload, Literal

import pandas as pd
from cognite.client import CogniteClient
from cognite.client.data_classes import TimeSeriesList, DatapointsList, Datapoints, DatapointsArrayList
from cognite.client.data_classes.datapoints import Aggregate
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from ._core import Aggregations, DEFAULT_LIMIT_READ, TypeAPI, IN_FILTER_LIMIT, INSTANCE_QUERY_LIMIT
from tutorial_apm_simple.client.data_classes import (
    Asset,
    AssetApply,
    AssetList,
    AssetApplyList,
    AssetFields,
    AssetTextFields,
    DomainModelApply,
)
from tutorial_apm_simple.client.data_classes._asset import _ASSET_PROPERTIES_BY_FIELD


ColumnNames = Literal[
    "areaId",
    "categoryId",
    "createdDate",
    "description",
    "isActive",
    "isCriticalLine",
    "pressure",
    "sourceDb",
    "specification",
    "tag",
    "trajectory",
    "updatedDate",
]


class AssetPressureQuery:
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
        """`Retrieve datapoints for the `asset.pressure` timeseries.

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
            we are using the time-ago format to get raw data for the 'my_pressure' from 2 weeks ago up until now::

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> asset_datapoints = client.asset.pressure(external_id="my_pressure").retrieve(start="2w-ago)
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
        """`Retrieve numpy arrays for the `asset.pressure` timeseries.

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
            we are using the time-ago format to get raw data for the 'my_pressure' from 2 weeks ago up until now::

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> asset_datapoints = client.asset.pressure(external_id="my_pressure").retrieve_array(start="2w-ago)
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
        column_names: ColumnNames | list[ColumnNames] = "pressure",
    ) -> pd.DataFrame:
        """`Retrieve DataFrames for the `asset.pressure` timeseries.

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
            column_names: Which property to use for column names. Defauts to pressure


        Returns:
            A ``DataFrame`` with the requested datapoints.

        Examples:

            In this example,
            we are using the time-ago format to get raw data for the 'my_pressure' from 2 weeks ago up until now::

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> asset_datapoints = client.asset.pressure(external_id="my_pressure").retrieve_dataframe(start="2w-ago)
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
        uniform_index: bool = False,
        include_aggregate_name: bool = True,
        include_granularity_name: bool = False,
        column_names: ColumnNames | list[ColumnNames] = "pressure",
    ) -> pd.DataFrame:
        """Retrieve DataFrames for the `asset.pressure` timeseries in Timezone.

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
            column_names: Which property to use for column names. Defauts to pressure


        Returns:
            A ``DataFrame`` with the requested datapoints.

        Examples:

            In this example,
            get weekly aggregates for the 'my_pressure' for the first month of 2023 in Oslo time:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> from datetime import datetime, timezone
                >>> client = ApmSimpleClient()
                >>> asset_datapoints = client.asset.pressure(
                ...     external_id="my_pressure").retrieve_dataframe_in_timezone(
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
        self, extra_properties: ColumnNames | list[ColumnNames] = "pressure"
    ) -> dict[str, list[str]]:
        return _retrieve_timeseries_external_ids_with_extra_pressure(
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
        if isinstance(column_names, str) and column_names == "pressure":
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


class AssetPressureAPI:
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        self._client = client
        self._view_id = view_id

    def __call__(
        self,
        min_area_id: int | None = None,
        max_area_id: int | None = None,
        min_category_id: int | None = None,
        max_category_id: int | None = None,
        min_created_date: datetime.datetime | None = None,
        max_created_date: datetime.datetime | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        is_active: bool | None = None,
        is_critical_line: bool | None = None,
        parent: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source_db: str | list[str] | None = None,
        source_db_prefix: str | None = None,
        tag: str | list[str] | None = None,
        tag_prefix: str | None = None,
        min_updated_date: datetime.datetime | None = None,
        max_updated_date: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> AssetPressureQuery:
        """Query timeseries `asset.pressure`

        Args:
            min_area_id: The minimum value of the area id to filter on.
            max_area_id: The maximum value of the area id to filter on.
            min_category_id: The minimum value of the category id to filter on.
            max_category_id: The maximum value of the category id to filter on.
            min_created_date: The minimum value of the created date to filter on.
            max_created_date: The maximum value of the created date to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            is_active: The is active to filter on.
            is_critical_line: The is critical line to filter on.
            parent: The parent to filter on.
            source_db: The source db to filter on.
            source_db_prefix: The prefix of the source db to filter on.
            tag: The tag to filter on.
            tag_prefix: The prefix of the tag to filter on.
            min_updated_date: The minimum value of the updated date to filter on.
            max_updated_date: The maximum value of the updated date to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of assets to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query object that can be used to retrieve datapoins for the  asset.pressure timeseries
            selected in this method.

        Examples:

            Retrieve all data for 5 asset.pressure timeseries:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> assets = client.asset.pressure(limit=5).retrieve()

        """
        filter_ = _create_filter(
            self._view_id,
            min_area_id,
            max_area_id,
            min_category_id,
            max_category_id,
            min_created_date,
            max_created_date,
            description,
            description_prefix,
            is_active,
            is_critical_line,
            parent,
            source_db,
            source_db_prefix,
            tag,
            tag_prefix,
            min_updated_date,
            max_updated_date,
            external_id_prefix,
            space,
            filter,
        )

        return AssetPressureQuery(
            client=self._client,
            view_id=self._view_id,
            timeseries_limit=limit,
            filter=filter_,
        )

    def list(
        self,
        min_area_id: int | None = None,
        max_area_id: int | None = None,
        min_category_id: int | None = None,
        max_category_id: int | None = None,
        min_created_date: datetime.datetime | None = None,
        max_created_date: datetime.datetime | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        is_active: bool | None = None,
        is_critical_line: bool | None = None,
        parent: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source_db: str | list[str] | None = None,
        source_db_prefix: str | None = None,
        tag: str | list[str] | None = None,
        tag_prefix: str | None = None,
        min_updated_date: datetime.datetime | None = None,
        max_updated_date: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> TimeSeriesList:
        """List timeseries `asset.pressure`

        Args:
            min_area_id: The minimum value of the area id to filter on.
            max_area_id: The maximum value of the area id to filter on.
            min_category_id: The minimum value of the category id to filter on.
            max_category_id: The maximum value of the category id to filter on.
            min_created_date: The minimum value of the created date to filter on.
            max_created_date: The maximum value of the created date to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            is_active: The is active to filter on.
            is_critical_line: The is critical line to filter on.
            parent: The parent to filter on.
            source_db: The source db to filter on.
            source_db_prefix: The prefix of the source db to filter on.
            tag: The tag to filter on.
            tag_prefix: The prefix of the tag to filter on.
            min_updated_date: The minimum value of the updated date to filter on.
            max_updated_date: The maximum value of the updated date to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of assets to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of Timeseries asset.pressure.

        Examples:

            List asset.pressure and limit to 5:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> assets = client.asset.pressure.list(limit=5)

        """
        filter_ = _create_filter(
            self._view_id,
            min_area_id,
            max_area_id,
            min_category_id,
            max_category_id,
            min_created_date,
            max_created_date,
            description,
            description_prefix,
            is_active,
            is_critical_line,
            parent,
            source_db,
            source_db_prefix,
            tag,
            tag_prefix,
            min_updated_date,
            max_updated_date,
            external_id_prefix,
            space,
            filter,
        )
        external_ids = _retrieve_timeseries_external_ids_with_extra_pressure(
            self._client, self._view_id, filter_, limit
        )
        if external_ids:
            return self._client.time_series.retrieve_multiple(external_ids=list(external_ids))
        else:
            return TimeSeriesList([])


def _retrieve_timeseries_external_ids_with_extra_pressure(
    client: CogniteClient,
    view_id: dm.ViewId,
    filter_: dm.Filter | None,
    limit: int,
    extra_properties: ColumnNames | list[ColumnNames] = "pressure",
) -> dict[str, list[str]]:
    properties = ["pressure"]
    if extra_properties == "pressure":
        ...
    elif isinstance(extra_properties, str) and extra_properties != "pressure":
        properties.append(extra_properties)
    elif isinstance(extra_properties, list):
        properties.extend([prop for prop in extra_properties if prop != "pressure"])
    else:
        raise ValueError(f"Invalid value for extra_properties: {extra_properties}")

    if isinstance(extra_properties, str):
        extra_list = [extra_properties]
    else:
        extra_list = extra_properties
    has_data = dm.filters.HasData([dm.ContainerId("tutorial_apm_simple", "Asset")], [view_id])
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
            node.properties[view_id]["pressure"]: [node.properties[view_id].get(prop, "") for prop in extra_list]
            for node in result.data["nodes"].data
        }
        total_retrieved += len(batch_external_ids)
        external_ids.update(batch_external_ids)
        cursor = result.cursors["nodes"]
        if total_retrieved >= limit or cursor is None:
            break
    return external_ids


class AssetChildrenAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space: str = "tutorial_apm_simple") -> dm.EdgeList:
        """Retrieve one or more children edges by id(s) of a asset.

        Args:
            external_id: External id or list of external ids source asset.
            space: The space where all the child edges are located.

        Returns:
            The requested child edges.

        Examples:

            Retrieve children edge by id:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> asset = client.asset.children.retrieve("my_children")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "tutorial_apm_simple", "externalId": "Asset.children"},
        )
        if isinstance(external_id, str):
            is_assets = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
        else:
            is_assets = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
        return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_assets))

    def list(
        self, asset_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space: str = "tutorial_apm_simple"
    ) -> dm.EdgeList:
        """List children edges of a asset.

        Args:
            asset_id: ID of the source asset.
            limit: Maximum number of child edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the child edges are located.

        Returns:
            The requested child edges.

        Examples:

            List 5 children edges connected to "my_asset":

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> asset = client.asset.children.list("my_asset", limit=5)

        """
        f = dm.filters
        filters = [
            f.Equals(
                ["edge", "type"],
                {"space": "tutorial_apm_simple", "externalId": "Asset.children"},
            )
        ]
        if asset_id:
            asset_ids = [asset_id] if isinstance(asset_id, str) else asset_id
            is_assets = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in asset_ids],
            )
            filters.append(is_assets)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class AssetInModelAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space: str = "cdf_3d_schema") -> dm.EdgeList:
        """Retrieve one or more in_model_3_d edges by id(s) of a asset.

        Args:
            external_id: External id or list of external ids source asset.
            space: The space where all the in model 3 d edges are located.

        Returns:
            The requested in model 3 d edges.

        Examples:

            Retrieve in_model_3_d edge by id:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> asset = client.asset.in_model_3_d.retrieve("my_in_model_3_d")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "tutorial_apm_simple", "externalId": "cdf3dEntityConnection"},
        )
        if isinstance(external_id, str):
            is_assets = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
        else:
            is_assets = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
        return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_assets))

    def list(
        self, asset_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space: str = "cdf_3d_schema"
    ) -> dm.EdgeList:
        """List in_model_3_d edges of a asset.

        Args:
            asset_id: ID of the source asset.
            limit: Maximum number of in model 3 d edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the in model 3 d edges are located.

        Returns:
            The requested in model 3 d edges.

        Examples:

            List 5 in_model_3_d edges connected to "my_asset":

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> asset = client.asset.in_model_3_d.list("my_asset", limit=5)

        """
        f = dm.filters
        filters = [
            f.Equals(
                ["edge", "type"],
                {"space": "tutorial_apm_simple", "externalId": "cdf3dEntityConnection"},
            )
        ]
        if asset_id:
            asset_ids = [asset_id] if isinstance(asset_id, str) else asset_id
            is_assets = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in asset_ids],
            )
            filters.append(is_assets)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class AssetAPI(TypeAPI[Asset, AssetApply, AssetList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[AssetApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Asset,
            class_apply_type=AssetApply,
            class_list=AssetList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class
        self.children = AssetChildrenAPI(client)
        self.in_model_3_d = AssetInModelAPI(client)
        self.pressure = AssetPressureAPI(client, view_id)

    def apply(self, asset: AssetApply | Sequence[AssetApply], replace: bool = False) -> dm.InstancesApplyResult:
        """Add or update (upsert) assets.

        Note: This method iterates through all nodes linked to asset and create them including the edges
        between the nodes. For example, if any of `children` or `in_model_3_d` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            asset: Asset or sequence of assets to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new asset:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> from tutorial_apm_simple.client.data_classes import AssetApply
                >>> client = ApmSimpleClient()
                >>> asset = AssetApply(external_id="my_asset", ...)
                >>> result = client.asset.apply(asset)

        """
        if isinstance(asset, AssetApply):
            instances = asset.to_instances_apply(self._view_by_write_class)
        else:
            instances = AssetApplyList(asset).to_instances_apply(self._view_by_write_class)
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(self, external_id: str | Sequence[str], space: str = "tutorial_apm_simple") -> dm.InstancesDeleteResult:
        """Delete one or more asset.

        Args:
            external_id: External id of the asset to delete.
            space: The space where all the asset are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete asset by id:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> client.asset.delete("my_asset")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Asset:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> AssetList:
        ...

    def retrieve(self, external_id: str | Sequence[str], space: str = "tutorial_apm_simple") -> Asset | AssetList:
        """Retrieve one or more assets by id(s).

        Args:
            external_id: External id or list of external ids of the assets.
            space: The space where all the assets are located.

        Returns:
            The requested assets.

        Examples:

            Retrieve asset by id:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> asset = client.asset.retrieve("my_asset")

        """
        if isinstance(external_id, str):
            asset = self._retrieve((space, external_id))

            child_edges = self.children.retrieve(external_id, space=space)
            asset.children = [edge.end_node.external_id for edge in child_edges]
            in_model_3_d_edges = self.in_model_3_d.retrieve(external_id, space=space)
            asset.in_model_3_d = [edge.end_node.external_id for edge in in_model_3_d_edges]

            return asset
        else:
            assets = self._retrieve([(space, ext_id) for ext_id in external_id])

            child_edges = self.children.retrieve(external_id, space=space)
            self._set_children(assets, child_edges)
            in_model_3_d_edges = self.in_model_3_d.retrieve(external_id, space=space)
            self._set_in_model_3_d(assets, in_model_3_d_edges)

            return assets

    def search(
        self,
        query: str,
        properties: AssetTextFields | Sequence[AssetTextFields] | None = None,
        min_area_id: int | None = None,
        max_area_id: int | None = None,
        min_category_id: int | None = None,
        max_category_id: int | None = None,
        min_created_date: datetime.datetime | None = None,
        max_created_date: datetime.datetime | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        is_active: bool | None = None,
        is_critical_line: bool | None = None,
        parent: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source_db: str | list[str] | None = None,
        source_db_prefix: str | None = None,
        tag: str | list[str] | None = None,
        tag_prefix: str | None = None,
        min_updated_date: datetime.datetime | None = None,
        max_updated_date: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> AssetList:
        """Search assets

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            min_area_id: The minimum value of the area id to filter on.
            max_area_id: The maximum value of the area id to filter on.
            min_category_id: The minimum value of the category id to filter on.
            max_category_id: The maximum value of the category id to filter on.
            min_created_date: The minimum value of the created date to filter on.
            max_created_date: The maximum value of the created date to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            is_active: The is active to filter on.
            is_critical_line: The is critical line to filter on.
            parent: The parent to filter on.
            source_db: The source db to filter on.
            source_db_prefix: The prefix of the source db to filter on.
            tag: The tag to filter on.
            tag_prefix: The prefix of the tag to filter on.
            min_updated_date: The minimum value of the updated date to filter on.
            max_updated_date: The maximum value of the updated date to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of assets to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `children` or `in_model_3_d` external ids for the assets. Defaults to True.

        Returns:
            Search results assets matching the query.

        Examples:

           Search for 'my_asset' in all text properties:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> assets = client.asset.search('my_asset')

        """
        filter_ = _create_filter(
            self._view_id,
            min_area_id,
            max_area_id,
            min_category_id,
            max_category_id,
            min_created_date,
            max_created_date,
            description,
            description_prefix,
            is_active,
            is_critical_line,
            parent,
            source_db,
            source_db_prefix,
            tag,
            tag_prefix,
            min_updated_date,
            max_updated_date,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _ASSET_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: AssetFields | Sequence[AssetFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: AssetTextFields | Sequence[AssetTextFields] | None = None,
        min_area_id: int | None = None,
        max_area_id: int | None = None,
        min_category_id: int | None = None,
        max_category_id: int | None = None,
        min_created_date: datetime.datetime | None = None,
        max_created_date: datetime.datetime | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        is_active: bool | None = None,
        is_critical_line: bool | None = None,
        parent: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source_db: str | list[str] | None = None,
        source_db_prefix: str | None = None,
        tag: str | list[str] | None = None,
        tag_prefix: str | None = None,
        min_updated_date: datetime.datetime | None = None,
        max_updated_date: datetime.datetime | None = None,
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
        property: AssetFields | Sequence[AssetFields] | None = None,
        group_by: AssetFields | Sequence[AssetFields] = None,
        query: str | None = None,
        search_properties: AssetTextFields | Sequence[AssetTextFields] | None = None,
        min_area_id: int | None = None,
        max_area_id: int | None = None,
        min_category_id: int | None = None,
        max_category_id: int | None = None,
        min_created_date: datetime.datetime | None = None,
        max_created_date: datetime.datetime | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        is_active: bool | None = None,
        is_critical_line: bool | None = None,
        parent: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source_db: str | list[str] | None = None,
        source_db_prefix: str | None = None,
        tag: str | list[str] | None = None,
        tag_prefix: str | None = None,
        min_updated_date: datetime.datetime | None = None,
        max_updated_date: datetime.datetime | None = None,
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
        property: AssetFields | Sequence[AssetFields] | None = None,
        group_by: AssetFields | Sequence[AssetFields] | None = None,
        query: str | None = None,
        search_property: AssetTextFields | Sequence[AssetTextFields] | None = None,
        min_area_id: int | None = None,
        max_area_id: int | None = None,
        min_category_id: int | None = None,
        max_category_id: int | None = None,
        min_created_date: datetime.datetime | None = None,
        max_created_date: datetime.datetime | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        is_active: bool | None = None,
        is_critical_line: bool | None = None,
        parent: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source_db: str | list[str] | None = None,
        source_db_prefix: str | None = None,
        tag: str | list[str] | None = None,
        tag_prefix: str | None = None,
        min_updated_date: datetime.datetime | None = None,
        max_updated_date: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across assets

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            min_area_id: The minimum value of the area id to filter on.
            max_area_id: The maximum value of the area id to filter on.
            min_category_id: The minimum value of the category id to filter on.
            max_category_id: The maximum value of the category id to filter on.
            min_created_date: The minimum value of the created date to filter on.
            max_created_date: The maximum value of the created date to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            is_active: The is active to filter on.
            is_critical_line: The is critical line to filter on.
            parent: The parent to filter on.
            source_db: The source db to filter on.
            source_db_prefix: The prefix of the source db to filter on.
            tag: The tag to filter on.
            tag_prefix: The prefix of the tag to filter on.
            min_updated_date: The minimum value of the updated date to filter on.
            max_updated_date: The maximum value of the updated date to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of assets to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `children` or `in_model_3_d` external ids for the assets. Defaults to True.

        Returns:
            Aggregation results.

        Examples:

            Count assets in space `my_space`:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> result = client.asset.aggregate("count", space="my_space")

        """

        filter_ = _create_filter(
            self._view_id,
            min_area_id,
            max_area_id,
            min_category_id,
            max_category_id,
            min_created_date,
            max_created_date,
            description,
            description_prefix,
            is_active,
            is_critical_line,
            parent,
            source_db,
            source_db_prefix,
            tag,
            tag_prefix,
            min_updated_date,
            max_updated_date,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _ASSET_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: AssetFields,
        interval: float,
        query: str | None = None,
        search_property: AssetTextFields | Sequence[AssetTextFields] | None = None,
        min_area_id: int | None = None,
        max_area_id: int | None = None,
        min_category_id: int | None = None,
        max_category_id: int | None = None,
        min_created_date: datetime.datetime | None = None,
        max_created_date: datetime.datetime | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        is_active: bool | None = None,
        is_critical_line: bool | None = None,
        parent: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source_db: str | list[str] | None = None,
        source_db_prefix: str | None = None,
        tag: str | list[str] | None = None,
        tag_prefix: str | None = None,
        min_updated_date: datetime.datetime | None = None,
        max_updated_date: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for assets

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            min_area_id: The minimum value of the area id to filter on.
            max_area_id: The maximum value of the area id to filter on.
            min_category_id: The minimum value of the category id to filter on.
            max_category_id: The maximum value of the category id to filter on.
            min_created_date: The minimum value of the created date to filter on.
            max_created_date: The maximum value of the created date to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            is_active: The is active to filter on.
            is_critical_line: The is critical line to filter on.
            parent: The parent to filter on.
            source_db: The source db to filter on.
            source_db_prefix: The prefix of the source db to filter on.
            tag: The tag to filter on.
            tag_prefix: The prefix of the tag to filter on.
            min_updated_date: The minimum value of the updated date to filter on.
            max_updated_date: The maximum value of the updated date to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of assets to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `children` or `in_model_3_d` external ids for the assets. Defaults to True.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_filter(
            self._view_id,
            min_area_id,
            max_area_id,
            min_category_id,
            max_category_id,
            min_created_date,
            max_created_date,
            description,
            description_prefix,
            is_active,
            is_critical_line,
            parent,
            source_db,
            source_db_prefix,
            tag,
            tag_prefix,
            min_updated_date,
            max_updated_date,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _ASSET_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        min_area_id: int | None = None,
        max_area_id: int | None = None,
        min_category_id: int | None = None,
        max_category_id: int | None = None,
        min_created_date: datetime.datetime | None = None,
        max_created_date: datetime.datetime | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        is_active: bool | None = None,
        is_critical_line: bool | None = None,
        parent: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source_db: str | list[str] | None = None,
        source_db_prefix: str | None = None,
        tag: str | list[str] | None = None,
        tag_prefix: str | None = None,
        min_updated_date: datetime.datetime | None = None,
        max_updated_date: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> AssetList:
        """List/filter assets

        Args:
            min_area_id: The minimum value of the area id to filter on.
            max_area_id: The maximum value of the area id to filter on.
            min_category_id: The minimum value of the category id to filter on.
            max_category_id: The maximum value of the category id to filter on.
            min_created_date: The minimum value of the created date to filter on.
            max_created_date: The maximum value of the created date to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            is_active: The is active to filter on.
            is_critical_line: The is critical line to filter on.
            parent: The parent to filter on.
            source_db: The source db to filter on.
            source_db_prefix: The prefix of the source db to filter on.
            tag: The tag to filter on.
            tag_prefix: The prefix of the tag to filter on.
            min_updated_date: The minimum value of the updated date to filter on.
            max_updated_date: The maximum value of the updated date to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of assets to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `children` or `in_model_3_d` external ids for the assets. Defaults to True.

        Returns:
            List of requested assets

        Examples:

            List assets and limit to 5:

                >>> from tutorial_apm_simple.client import ApmSimpleClient
                >>> client = ApmSimpleClient()
                >>> assets = client.asset.list(limit=5)

        """
        filter_ = _create_filter(
            self._view_id,
            min_area_id,
            max_area_id,
            min_category_id,
            max_category_id,
            min_created_date,
            max_created_date,
            description,
            description_prefix,
            is_active,
            is_critical_line,
            parent,
            source_db,
            source_db_prefix,
            tag,
            tag_prefix,
            min_updated_date,
            max_updated_date,
            external_id_prefix,
            space,
            filter,
        )

        assets = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            if len(external_ids := assets.as_external_ids()) > IN_FILTER_LIMIT:
                child_edges = self.children.list(limit=-1, space=space)
            else:
                child_edges = self.children.list(external_ids, limit=-1, space=space)
            self._set_children(assets, child_edges)
            if len(external_ids := assets.as_external_ids()) > IN_FILTER_LIMIT:
                in_model_3_d_edges = self.in_model_3_d.list(limit=-1, space=space)
            else:
                in_model_3_d_edges = self.in_model_3_d.list(external_ids, limit=-1, space=space)
            self._set_in_model_3_d(assets, in_model_3_d_edges)

        return assets

    @staticmethod
    def _set_children(assets: Sequence[Asset], child_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in child_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for asset in assets:
            node_id = asset.id_tuple()
            if node_id in edges_by_start_node:
                asset.children = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]

    @staticmethod
    def _set_in_model_3_d(assets: Sequence[Asset], in_model_3_d_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in in_model_3_d_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for asset in assets:
            node_id = asset.id_tuple()
            if node_id in edges_by_start_node:
                asset.in_model_3_d = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


def _create_filter(
    view_id: dm.ViewId,
    min_area_id: int | None = None,
    max_area_id: int | None = None,
    min_category_id: int | None = None,
    max_category_id: int | None = None,
    min_created_date: datetime.datetime | None = None,
    max_created_date: datetime.datetime | None = None,
    description: str | list[str] | None = None,
    description_prefix: str | None = None,
    is_active: bool | None = None,
    is_critical_line: bool | None = None,
    parent: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    source_db: str | list[str] | None = None,
    source_db_prefix: str | None = None,
    tag: str | list[str] | None = None,
    tag_prefix: str | None = None,
    min_updated_date: datetime.datetime | None = None,
    max_updated_date: datetime.datetime | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if min_area_id or max_area_id:
        filters.append(dm.filters.Range(view_id.as_property_ref("areaId"), gte=min_area_id, lte=max_area_id))
    if min_category_id or max_category_id:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("categoryId"), gte=min_category_id, lte=max_category_id)
        )
    if min_created_date or max_created_date:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("createdDate"),
                gte=min_created_date.isoformat(timespec="milliseconds") if min_created_date else None,
                lte=max_created_date.isoformat(timespec="milliseconds") if max_created_date else None,
            )
        )
    if description and isinstance(description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("description"), value=description))
    if description and isinstance(description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("description"), values=description))
    if description_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("description"), value=description_prefix))
    if is_active and isinstance(is_active, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("isActive"), value=is_active))
    if is_critical_line and isinstance(is_critical_line, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("isCriticalLine"), value=is_critical_line))
    if parent and isinstance(parent, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("parent"), value={"space": "tutorial_apm_simple", "externalId": parent}
            )
        )
    if parent and isinstance(parent, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("parent"), value={"space": parent[0], "externalId": parent[1]})
        )
    if parent and isinstance(parent, list) and isinstance(parent[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("parent"),
                values=[{"space": "tutorial_apm_simple", "externalId": item} for item in parent],
            )
        )
    if parent and isinstance(parent, list) and isinstance(parent[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("parent"), values=[{"space": item[0], "externalId": item[1]} for item in parent]
            )
        )
    if source_db and isinstance(source_db, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("sourceDb"), value=source_db))
    if source_db and isinstance(source_db, list):
        filters.append(dm.filters.In(view_id.as_property_ref("sourceDb"), values=source_db))
    if source_db_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("sourceDb"), value=source_db_prefix))
    if tag and isinstance(tag, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("tag"), value=tag))
    if tag and isinstance(tag, list):
        filters.append(dm.filters.In(view_id.as_property_ref("tag"), values=tag))
    if tag_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("tag"), value=tag_prefix))
    if min_updated_date or max_updated_date:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("updatedDate"),
                gte=min_updated_date.isoformat(timespec="milliseconds") if min_updated_date else None,
                lte=max_updated_date.isoformat(timespec="milliseconds") if max_updated_date else None,
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
