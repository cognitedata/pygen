from __future__ import annotations

from collections.abc import Sequence
from typing import Generic, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeriesList
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList
from movie_domain.client.data_classes._core import (
    DomainModelApply,
    ResourcesApplyResult,
    T_DomainModel,
    T_DomainModelApply,
    T_DomainModelList,
)

DEFAULT_LIMIT_READ = 25
INSTANCE_QUERY_LIMIT = 1_000
IN_FILTER_LIMIT = 5_000

Aggregations = Literal["avg", "count", "max", "min", "sum"]

_METRIC_AGGREGATIONS_BY_NAME = {
    "avg": dm.aggregations.Avg,
    "count": dm.aggregations.Count,
    "max": dm.aggregations.Max,
    "min": dm.aggregations.Min,
    "sum": dm.aggregations.Sum,
}


class TypeAPI(Generic[T_DomainModel, T_DomainModelApply, T_DomainModelList]):
    def __init__(
        self,
        client: CogniteClient,
        sources: dm.ViewIdentifier | Sequence[dm.ViewIdentifier] | dm.View | Sequence[dm.View],
        class_type: type[T_DomainModel],
        class_apply_type: type[T_DomainModelApply],
        class_list: type[T_DomainModelList],
        view_by_write_class: dict[type[DomainModelApply], dm.ViewId],
    ):
        self._client = client
        self._sources = sources
        self._class_type = class_type
        self._class_apply_type = class_apply_type
        self._class_list = class_list
        self._view_by_write_class = view_by_write_class

    def _apply(
        self, item: T_DomainModelApply | Sequence[T_DomainModelApply], replace: bool = False
    ) -> ResourcesApplyResult:
        if isinstance(item, DomainModelApply):
            instances = item.to_instances_apply(self._view_by_write_class)
        else:
            instances = self._class_list(item).to_instances_apply(self._view_by_write_class)
        result = self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )
        time_series = []
        if instances.time_series:
            time_series = self._client.time_series.upsert(instances.time_series, mode="patch")

        return ResourcesApplyResult(result.nodes, result.edges, TimeSeriesList(time_series))

    @overload
    def _retrieve(self, nodes: tuple[str, str]) -> T_DomainModel:
        ...

    @overload
    def _retrieve(self, nodes: Sequence[tuple[str, str]]) -> T_DomainModelList:
        ...

    def _retrieve(self, nodes: tuple[str, str] | Sequence[tuple[str, str]]) -> T_DomainModel | T_DomainModelList:
        is_multiple = (
            isinstance(nodes, Sequence)
            and not isinstance(nodes, str)
            and not (isinstance(nodes, tuple) and isinstance(nodes[0], str))
        )
        instances = self._client.data_modeling.instances.retrieve(nodes=nodes, sources=self._sources)
        if is_multiple:
            return self._class_list([self._class_type.from_node(node) for node in instances.nodes])
        return self._class_type.from_node(instances.nodes[0])

    def _search(
        self,
        view_id: dm.ViewId,
        query: str,
        properties_by_field: dict[str, str],
        properties: str | Sequence[str],
        filter_: dm.Filter | None = None,
        limit: int = DEFAULT_LIMIT_READ,
    ) -> T_DomainModelList:
        if isinstance(properties, str):
            properties = [properties]

        if properties:
            properties = [properties_by_field.get(prop, prop) for prop in properties]

        nodes = self._client.data_modeling.instances.search(view_id, query, "node", properties, filter_, limit)
        return self._class_list([self._class_type.from_node(node) for node in nodes])

    @overload
    def _aggregate(
        self,
        view_id: dm.ViewId,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        properties_by_field: dict[str, str],
        properties: str | Sequence[str] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: str | Sequence[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

    @overload
    def _aggregate(
        self,
        view_id: dm.ViewId,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        properties_by_field: dict[str, str],
        properties: str | Sequence[str] = None,
        group_by: str | Sequence[str] | None = None,
        query: str | None = None,
        search_properties: str | Sequence[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList:
        ...

    def _aggregate(
        self,
        view_id: dm.ViewId,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        properties_by_field: dict[str, str],
        properties: str | Sequence[str] | None = None,
        group_by: str | Sequence[str] | None = None,
        query: str | None = None,
        search_properties: str | Sequence[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        if isinstance(group_by, str):
            group_by = [group_by]

        if group_by:
            group_by = [properties_by_field.get(prop, prop) for prop in group_by]

        if isinstance(search_properties, str):
            search_properties = [search_properties]

        if search_properties:
            search_properties = [properties_by_field.get(prop, prop) for prop in search_properties]

        if isinstance(properties, str):
            properties = [properties]

        if properties:
            properties = [properties_by_field.get(prop, prop) for prop in properties]

        if isinstance(aggregate, (str, dm.aggregations.MetricAggregation)):
            aggregate = [aggregate]

        if properties is None and (invalid := [agg for agg in aggregate if isinstance(agg, str) and agg != "count"]):
            raise ValueError(f"Cannot aggregate on {invalid} without specifying properties")

        aggregates = []
        for agg in aggregate:
            if isinstance(agg, dm.aggregations.MetricAggregation):
                aggregates.append(agg)
            elif isinstance(agg, str):
                if agg == "count" and properties is None:
                    # Special case for count, we just pick the first property
                    first_prop = next(iter(properties_by_field.values()))
                    aggregates.append(dm.aggregations.Count(first_prop))
                elif properties is None:
                    raise ValueError(f"Cannot aggregate on {agg} without specifying properties")
                else:
                    for prop in properties:
                        aggregates.append(_METRIC_AGGREGATIONS_BY_NAME[agg](prop))
            else:
                raise TypeError(f"Expected str or MetricAggregation, got {type(agg)}")

        result = self._client.data_modeling.instances.aggregate(
            view_id, aggregates, "node", group_by, query, search_properties, filter, limit
        )
        if group_by is None:
            return result[0].aggregates
        return result

    def _histogram(
        self,
        view_id: dm.ViewId,
        property: str,
        interval: float,
        properties_by_field: dict[str, str],
        query: str | None = None,
        search_properties: str | Sequence[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        property = properties_by_field.get(property, property)

        if isinstance(search_properties, str):
            search_properties = [search_properties]
        if search_properties:
            search_properties = [properties_by_field.get(prop, prop) for prop in search_properties]

        return self._client.data_modeling.instances.histogram(
            view_id, dm.aggregations.Histogram(property, interval), "node", query, search_properties, filter, limit
        )

    def _list(self, limit: int = DEFAULT_LIMIT_READ, filter: dm.Filter | None = None) -> T_DomainModelList:
        nodes = self._client.data_modeling.instances.list("node", sources=self._sources, limit=limit, filter=filter)
        return self._class_list([self._class_type.from_node(node) for node in nodes])
