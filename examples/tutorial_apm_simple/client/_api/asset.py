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

from ._core import DEFAULT_LIMIT_READ, TypeAPI, INSTANCE_QUERY_LIMIT
from tutorial_apm_simple.client.data_classes import Asset, AssetApply, AssetList, AssetApplyList

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
        start: int | str | datetime.datetime | None = None,
        end: int | str | datetime.datetime | None = None,
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
        start: int | str | datetime.datetime | None = None,
        end: int | str | datetime.datetime | None = None,
        *,
        aggregates: Aggregate | list[Aggregate] | None = None,
        granularity: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
        uniform_index: bool = False,
        include_aggregate_name: bool = True,
        include_granularity_name: bool = False,
        column_names: ColumnNames | list[ColumnNames] = "pressure",
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

    def plot(
        self,
        start: int | str | datetime.datetime | None = None,
        end: int | str | datetime.datetime | None = None,
        *,
        aggregates: Aggregate | Sequence[Aggregate] | None = None,
        granularity: str | None = None,
        uniform_index: bool = False,
        include_aggregate_name: bool = True,
        include_granularity_name: bool = False,
        column_names: ColumnNames | list[ColumnNames] = "pressure",
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
        source_db: str | list[str] | None = None,
        source_db_prefix: str | None = None,
        tag: str | list[str] | None = None,
        tag_prefix: str | None = None,
        min_updated_date: datetime.datetime | None = None,
        max_updated_date: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> AssetPressureQuery:
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
            source_db,
            source_db_prefix,
            tag,
            tag_prefix,
            min_updated_date,
            max_updated_date,
            external_id_prefix,
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
        source_db: str | list[str] | None = None,
        source_db_prefix: str | None = None,
        tag: str | list[str] | None = None,
        tag_prefix: str | None = None,
        min_updated_date: datetime.datetime | None = None,
        max_updated_date: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> TimeSeriesList:
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
            source_db,
            source_db_prefix,
            tag,
            tag_prefix,
            min_updated_date,
            max_updated_date,
            external_id_prefix,
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

    def retrieve(self, external_id: str | Sequence[str], space="tutorial_apm_simple") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "Asset.children"},
        )
        if isinstance(external_id, str):
            is_asset = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_asset))

        else:
            is_assets = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_assets))

    def list(
        self, asset_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="tutorial_apm_simple"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "Asset.children"},
        )
        filters.append(is_edge_type)
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

    def retrieve(self, external_id: str | Sequence[str], space="cdf_3d_schema") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "cdf3dEntityConnection"},
        )
        if isinstance(external_id, str):
            is_asset = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_asset))

        else:
            is_assets = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_assets))

    def list(
        self, asset_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="cdf_3d_schema"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "cdf3dEntityConnection"},
        )
        filters.append(is_edge_type)
        if asset_id:
            asset_ids = [asset_id] if isinstance(asset_id, str) else asset_id
            is_assets = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in asset_ids],
            )
            filters.append(is_assets)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class AssetAPI(TypeAPI[Asset, AssetApply, AssetList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Asset,
            class_apply_type=AssetApply,
            class_list=AssetList,
        )
        self._view_id = view_id
        self.children = AssetChildrenAPI(client)
        self.in_model_3_d = AssetInModelAPI(client)
        self.pressure = AssetPressureAPI(client, view_id)

    def apply(self, asset: AssetApply | Sequence[AssetApply], replace: bool = False) -> dm.InstancesApplyResult:
        if isinstance(asset, AssetApply):
            instances = asset.to_instances_apply()
        else:
            instances = AssetApplyList(asset).to_instances_apply()
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(self, external_id: str | Sequence[str], space="tutorial_apm_simple") -> dm.InstancesDeleteResult:
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

    def retrieve(self, external_id: str | Sequence[str]) -> Asset | AssetList:
        if isinstance(external_id, str):
            asset = self._retrieve((self._sources.space, external_id))

            child_edges = self.children.retrieve(external_id)
            asset.children = [edge.end_node.external_id for edge in child_edges]
            in_model_3_d_edges = self.in_model_3_d.retrieve(external_id)
            asset.in_model_3_d = [edge.end_node.external_id for edge in in_model_3_d_edges]

            return asset
        else:
            assets = self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

            child_edges = self.children.retrieve(external_id)
            self._set_children(assets, child_edges)
            in_model_3_d_edges = self.in_model_3_d.retrieve(external_id)
            self._set_in_model_3_d(assets, in_model_3_d_edges)

            return assets

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
        source_db: str | list[str] | None = None,
        source_db_prefix: str | None = None,
        tag: str | list[str] | None = None,
        tag_prefix: str | None = None,
        min_updated_date: datetime.datetime | None = None,
        max_updated_date: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = False,
    ) -> AssetList:
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
            source_db,
            source_db_prefix,
            tag,
            tag_prefix,
            min_updated_date,
            max_updated_date,
            external_id_prefix,
            filter,
        )

        assets = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            child_edges = self.children.list(assets.as_external_ids(), limit=-1)
            self._set_children(assets, child_edges)
            in_model_3_d_edges = self.in_model_3_d.list(assets.as_external_ids(), limit=-1)
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
    source_db: str | list[str] | None = None,
    source_db_prefix: str | None = None,
    tag: str | list[str] | None = None,
    tag_prefix: str | None = None,
    min_updated_date: datetime.datetime | None = None,
    max_updated_date: datetime.datetime | None = None,
    external_id_prefix: str | None = None,
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
                gte=min_created_date.isoformat() if min_created_date else None,
                lte=max_created_date.isoformat() if max_created_date else None,
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
                gte=min_updated_date.isoformat() if min_updated_date else None,
                lte=max_updated_date.isoformat() if max_updated_date else None,
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
