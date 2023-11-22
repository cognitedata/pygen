from __future__ import annotations

from collections import defaultdict, UserList
from collections.abc import Sequence, Collection
from typing import Generic, Literal, Any, Iterator, Protocol, SupportsIndex, TypeVar, overload
from dataclasses import dataclass, field
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeriesList
from cognite.client.data_classes.data_modeling.instances import Instance
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList
from equipment_unit.client.data_classes._core import (
    DomainModel,
    DomainModelList,
    DomainModelApply,
    DomainRelationApply,
    ResourcesApplyResult,
    T_DomainModel,
    T_DomainModelApply,
    T_DomainModelList,
    T_DomainRelation,
    T_DomainRelationApply,
    T_DomainRelationList,
    DomainModelCore,
    DomainRelation,
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

_T_co = TypeVar("_T_co", covariant=True)


# Source from https://github.com/python/typing/issues/256#issuecomment-1442633430
# This works because str.__contains__ does not accept an object (either in typeshed or at runtime)
class SequenceNotStr(Protocol[_T_co]):
    @overload
    def __getitem__(self, index: SupportsIndex, /) -> _T_co:
        ...

    @overload
    def __getitem__(self, index: slice, /) -> Sequence[_T_co]:
        ...

    def __contains__(self, value: object, /) -> bool:
        ...

    def __len__(self) -> int:
        ...

    def __iter__(self) -> Iterator[_T_co]:
        ...

    def index(self, value: Any, /, start: int = 0, stop: int = ...) -> int:
        ...

    def count(self, value: Any, /) -> int:
        ...

    def __reversed__(self) -> Iterator[_T_co]:
        ...


class NodeAPI(Generic[T_DomainModel, T_DomainModelApply, T_DomainModelList]):
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

    def _delete(self, external_id: str | Sequence[str], space: str) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def _retrieve(
        self,
        external_id: str,
        space: str,
        retrieve_edges: bool = False,
        edge_api_name_pairs: list[tuple[EdgeAPI, str]] | None = None,
    ) -> T_DomainModel:
        ...

    @overload
    def _retrieve(
        self,
        external_id: SequenceNotStr[str],
        space: str,
        retrieve_edges: bool = False,
        edge_api_name_pairs: list[tuple[EdgeAPI, str]] | None = None,
    ) -> T_DomainModelList:
        ...

    def _retrieve(
        self,
        external_id: str | SequenceNotStr[str],
        space: str,
        retrieve_edges: bool = False,
        edge_api_name_pairs: list[tuple[EdgeAPI, str]] = None,
    ) -> T_DomainModel | T_DomainModelList:
        is_multiple = True
        if isinstance(external_id, str):
            node_ids = (space, external_id)
            is_multiple = False
        else:
            node_ids = [(space, ext_id) for ext_id in external_id]

        instances = self._client.data_modeling.instances.retrieve(nodes=node_ids, sources=self._sources)
        nodes = self._class_list([self._class_type.from_node(node) for node in instances.nodes])

        if retrieve_edges:
            self._retrieve_and_set_edge_types(nodes, space, edge_api_name_pairs)

        if is_multiple:
            return nodes
        else:
            return nodes[0]

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

    def _list(
        self,
        limit: int,
        filter: dm.Filter,
        retrieve_edges: bool = False,
        space: str | None = None,
        edge_api_name_pairs: list[tuple[EdgeAPI, str]] | None = None,
    ) -> T_DomainModelList:
        nodes = self._client.data_modeling.instances.list("node", sources=self._sources, limit=limit, filter=filter)
        node_list = self._class_list([self._class_type.from_node(node) for node in nodes])
        if retrieve_edges:
            self._retrieve_and_set_edge_types(node_list, space, edge_api_name_pairs)

        return node_list

    @classmethod
    def _retrieve_and_set_edge_types(
        cls, nodes: T_DomainModelList, space: str | None, edge_api_name_pairs: list[tuple[EdgeAPI, str]] | None = None
    ):
        for edge_api, edge_name in edge_api_name_pairs or []:
            space_arg = {"space": space} if space else {}
            if len(ids := nodes.as_node_ids()) > IN_FILTER_LIMIT:
                edges = edge_api._list(limit=-1, **space_arg)
            else:
                edges = edge_api._list(node_ids=ids, limit=-1)
            cls._set_edges(nodes, edges, edge_name)

    @staticmethod
    def _set_edges(nodes: Sequence[DomainModel], edges: Sequence[dm.Edge], edge_name: str):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for node in nodes:
            node_id = node.as_tuple_id()
            if node_id in edges_by_start_node:
                setattr(node, edge_name, [edge.end_node.external_id for edge in edges_by_start_node[node_id]])


class EdgeAPI(Generic[T_DomainRelation, T_DomainRelationApply, T_DomainRelationList]):
    def __init__(
        self,
        client: CogniteClient,
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId],
        class_type: type[T_DomainRelation],
        class_apply_type: type[T_DomainRelationApply],
        class_list: type[T_DomainRelationList],
    ):
        self._client = client
        self._view_by_write_class = view_by_write_class
        self._view_id = view_by_write_class[class_apply_type]
        self._class_type = class_type
        self._class_apply_type = class_apply_type
        self._class_list = class_list

    def _list(
        self,
        node_ids: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filters: list[dm.Filter] | None = None,
        space: str | None = None,
    ) -> T_DomainRelationList:
        filters = filters or []
        if node_ids and isinstance(node_ids, str):
            filters.append(dm.filters.Equals(["edge", "startNode"], value={"space": space, "externalId": node_ids}))
        elif node_ids and isinstance(node_ids, dm.NodeId):
            filters.append(
                dm.filters.Equals(
                    ["edge", "startNode"], value=node_ids.dump(camel_case=True, include_instance_type=False)
                )
            )
        if node_ids and isinstance(node_ids, list):
            filters.append(
                dm.filters.In(
                    ["edge", "startNode"],
                    values=[
                        {"space": space, "externalId": ext_id}
                        if isinstance(ext_id, str)
                        else ext_id.dump(camel_case=True, include_instance_type=False)
                        for ext_id in node_ids
                    ],
                )
            )
        if space and isinstance(space, str):
            filters.append(dm.filters.Equals(["edge", "space"], value=space))
        edges = self._client.data_modeling.instances.list(
            "edge", limit=limit, filter=dm.filters.And(*filters), sources=[self._view_id]
        )
        return self._class_list([self._class_type.from_edge(edge) for edge in edges])


@dataclass
class QueryExpression:
    # Setup Variables
    name: str
    filter: dm.Filter | None
    select: dm.query.Select
    from_: str | None
    expression_cls: type[dm.query.ResultSetExpression]
    result_cls: type[DomainModelCore] | None
    max_retrieve_limit: int

    # Query Variables
    cursor: str | None = None
    total_retrieved: int = 0
    results: list[Instance] = field(default_factory=list)
    last_batch_count: int = 0

    @property
    def limit(self) -> int:
        return min(INSTANCE_QUERY_LIMIT, self.max_retrieve_limit - self.total_retrieved)


@dataclass
class EdgeLike(Protocol):
    start_node: dm.DirectRelationReference
    end_node: dm.DirectRelationReference


class QueryBuilder(UserList, Generic[T_DomainModelList]):
    def __init__(self, result_cls: type[T_DomainModelList], nodes: Collection[QueryExpression] = None):
        super().__init__(nodes or [])
        self._result_cls = result_cls

    # The dunder implementations are to get proper type hints
    def __iter__(self) -> Iterator[QueryExpression]:
        return super().__iter__()

    @overload
    def __getitem__(self, item: int) -> QueryExpression:
        ...

    @overload
    def __getitem__(self: type[QueryBuilder[T_DomainModelList]], item: slice) -> QueryBuilder[T_DomainModelList]:
        ...

    def __getitem__(self, item: int | slice) -> QueryExpression | QueryBuilder[T_DomainModelList]:
        if isinstance(item, slice):
            return self.__class__(self.data[item])
        elif isinstance(item, int):
            return self.data[item]
        else:
            raise TypeError(f"Expected int or slice, got {type(item)}")

    def reset(self):
        for expression in self:
            expression.total_retrieved = 0
            expression.cursor = None
            expression.results = []

    def build(self) -> dm.query.Query:
        with_ = {
            expression.name: expression.expression_cls(
                from_=expression.from_, filter=expression.filter, limit=expression.limit, sort=None
            )
            for expression in self
        }
        select = {expression.name: expression.select for expression in self}
        cursors = {expression.name: expression.cursor for expression in self}

        return dm.query.Query(with_=with_, select=select, cursors=cursors)

    def update(self, batch: dm.query.QueryResult):
        for expression in self:
            expression.last_batch_count = len(batch[expression.name])
            expression.total_retrieved += expression.last_batch_count
            expression.cursor = batch.cursors.get(expression.name)
            expression.results.extend(batch[expression.name].data)

    @property
    def is_finished(self):
        return all(
            expression.total_retrieved >= expression.max_retrieve_limit
            or expression.cursor is None
            or expression.last_batch_count == 0
            for expression in self
        )

    def unpack(self) -> T_DomainModelList:
        nodes_by_type: dict[str | None, dict[tuple[str, str], DomainModel]] = defaultdict(dict)
        edges_by_type_by_start_node: dict[tuple[str, str], dict[tuple[str, str], list[EdgeLike]]] = defaultdict(
            lambda: defaultdict(list)
        )

        for expression in self:
            if issubclass(expression.result_cls, DomainModel):
                for node in expression.results:
                    domain = expression.result_cls.from_instance(node)
                    # Circular dependencies will overwrite here, so we always get the last one
                    nodes_by_type[expression.name][domain.as_tuple_id()] = domain
            elif issubclass(expression.result_cls, DomainRelation) or expression.result_cls is None:
                for edge in expression.results:
                    domain = expression.result_cls.from_instance(edge) if expression.result_cls else edge
                    edges_by_type_by_start_node[(expression.from_, expression.name)][
                        domain.start_node.as_tuple()
                    ].append(domain)

        for (node_name, node_attribute), edges_by_start_node in edges_by_type_by_start_node.items():
            for node in nodes_by_type[node_name].values():
                setattr(node, node_attribute, edges_by_start_node.get(node.as_tuple_id(), []))

        return self._result_cls(nodes_by_type[self[0].name].values())


class QueryAPI:
    def __init__(self, client: CogniteClient, builder: QueryBuilder, from_: str):
        self._client = client
        self._builder = builder
        self._from = from_

    def _query(self) -> DomainModelList:
        self._builder.reset()
        while True:
            query = self._builder.build()
            batch = self._client.data_modeling.instances.query(query)
            self._builder.update(batch)
            if self._builder.is_finished:
                break
        return self._builder.unpack()
