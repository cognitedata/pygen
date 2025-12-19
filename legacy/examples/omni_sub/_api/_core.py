from __future__ import annotations

from abc import ABC
from collections import defaultdict
from collections.abc import Sequence
from itertools import groupby
from typing import (
    Generic,
    Literal,
    Any,
    Iterator,
    Protocol,
    SupportsIndex,
    TypeVar,
    overload,
    ClassVar,
)

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeriesList
from cognite.client.data_classes.data_modeling.instances import InstanceSort, InstanceAggregationResultList
from pydantic import BaseModel, TypeAdapter, ValidationError

from omni_sub.config import global_config
from omni_sub import data_classes
from omni_sub.data_classes._core import (
    chunker,
    DomainModel,
    DomainModelWrite,
    IN_FILTER_CHUNK_SIZE,
    PageInfo,
    GraphQLCore,
    GraphQLList,
    ResourcesWriteResult,
    T_DomainModel,
    T_DomainModelWrite,
    T_DomainModelWriteList,
    T_DomainModelList,
    T_DomainRelation,
    T_DomainRelationWrite,
    T_DomainRelationList,
    QueryBuilder,
    QueryExecutor,
    QueryUnpacker,
)

DEFAULT_LIMIT_READ = 25
DEFAULT_CHUNK_SIZE = 100
DEFAULT_QUERY_LIMIT = 3
IN_FILTER_LIMIT = 5_000
INSTANCE_QUERY_LIMIT = 1_000
NODE_PROPERTIES = {"externalId", "space"}

Aggregations = Literal["avg", "count", "max", "min", "sum"]

_METRIC_AGGREGATIONS_BY_NAME = {
    "avg": dm.aggregations.Avg,
    "count": dm.aggregations.Count,
    "max": dm.aggregations.Max,
    "min": dm.aggregations.Min,
    "sum": dm.aggregations.Sum,
}

_T_co = TypeVar("_T_co", covariant=True)


def _as_node_id(value: str | dm.NodeId | tuple[str, str], space: str) -> dm.NodeId:
    if isinstance(value, str):
        return dm.NodeId(space=space, external_id=value)
    if isinstance(value, dm.NodeId):
        return value
    if isinstance(value, tuple):
        return dm.NodeId(space=value[0], external_id=value[1])
    raise TypeError(f"Expected str, NodeId or tuple, got {type(value)}")


# Source from https://github.com/python/typing/issues/256#issuecomment-1442633430
# This works because str.__contains__ does not accept an object (either in typeshed or at runtime)
class SequenceNotStr(Protocol[_T_co]):
    @overload
    def __getitem__(self, index: SupportsIndex, /) -> _T_co: ...

    @overload
    def __getitem__(self, index: slice, /) -> Sequence[_T_co]: ...

    def __contains__(self, value: object, /) -> bool: ...

    def __len__(self) -> int: ...

    def __iter__(self) -> Iterator[_T_co]: ...

    def index(self, value: Any, /, start: int = 0, stop: int = ...) -> int: ...

    def count(self, value: Any, /) -> int: ...

    def __reversed__(self) -> Iterator[_T_co]: ...


class NodeReadAPI(Generic[T_DomainModel, T_DomainModelList], ABC):
    _view_id: ClassVar[dm.ViewId]
    _properties_by_field: ClassVar[dict[str, str]]
    _direct_children_by_external_id: ClassVar[dict[str, type[DomainModel]]]
    _class_type: type[T_DomainModel]
    _class_list: type[T_DomainModelList]

    def __init__(self, client: CogniteClient):
        self._client = client
        self._last_cursors: dict[str, str | None] | None = None

    def _delete(self, external_id: str | SequenceNotStr[str], space: str) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    def _retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
        as_child_class: SequenceNotStr[str] | None = None,
    ) -> T_DomainModel | T_DomainModelList | None:
        if isinstance(external_id, str | dm.NodeId) or (
            isinstance(external_id, tuple) and len(external_id) == 2 and all(isinstance(i, str) for i in external_id)
        ):
            node_ids = [_as_node_id(external_id, space)]
            is_multiple = False
        else:
            is_multiple = True
            node_ids = [_as_node_id(ext_id, space) for ext_id in external_id]

        items: list[DomainModel] = []
        if as_child_class and retrieve_connections == "skip":
            if not hasattr(self, "_direct_children_by_external_id"):
                raise ValueError(f"{type(self).__name__} does not have any direct children")
            for child_class_external_id in as_child_class:
                child_cls = self._direct_children_by_external_id.get(child_class_external_id)
                if child_cls is None:
                    raise ValueError(f"Could not find child class with external_id {child_class_external_id}")
                instances = self._client.data_modeling.instances.retrieve(nodes=node_ids, sources=child_cls._view_id)
                items.extend(
                    instantiate_classes(child_cls, [child_cls._to_dict(node) for node in instances.nodes], "retrieve")
                )
        elif as_child_class:
            raise ValueError("Cannot retrieve as child classes and include connections")
        elif retrieve_connections == "skip":
            instances = self._client.data_modeling.instances.retrieve(nodes=node_ids, sources=self._view_id)
            items.extend(
                instantiate_classes(
                    self._class_type, [self._class_type._to_dict(node) for node in instances.nodes], "retrieve"
                )
            )
        else:
            for space_key, external_ids in groupby(
                sorted((node_id.as_tuple() for node_id in node_ids)), key=lambda x: x[0]
            ):
                external_id_list = [ext_id[1] for ext_id in external_ids]
                for ext_id_chunk in chunker(external_id_list, IN_FILTER_CHUNK_SIZE):
                    filter_ = dm.filters.Equals(["node", "space"], space_key) & dm.filters.In(
                        ["node", "externalId"], ext_id_chunk
                    )
                    items.extend(self._query(filter_, len(ext_id_chunk), retrieve_connections, None, "retrieve"))

        nodes = self._class_list(items)

        if is_multiple:
            return nodes
        elif not nodes:
            return None
        else:
            return nodes[0]

    def _build(
        self,
        filter_: dm.Filter | None,
        limit: int | None,
        retrieve_connections: Literal["skip", "identifier", "full"],
        sort: list[InstanceSort] | None = None,
        chunk_size: int | None = None,
    ) -> QueryExecutor:
        raise NotImplementedError

    def _query(
        self,
        filter_: dm.Filter | None,
        limit: int,
        retrieve_connections: Literal["skip", "identifier", "full"],
        sort: list[InstanceSort] | None = None,
        context: Literal["query", "list", "retrieve"] = "query",
    ) -> T_DomainModelList:
        executor = self._build(filter_, limit, retrieve_connections, sort)
        results = executor.execute_query(self._client, remove_not_connected=False)
        unpacked = QueryUnpacker(results, edges="skip").unpack()
        item_list = instantiate_classes(self._class_type, unpacked, context)
        return self._class_list(item_list)

    def _iterate(
        self,
        chunk_size: int,
        filter_: dm.Filter | None,
        limit: int | None,
        retrieve_connections: Literal["skip", "identifier", "full"],
        sort: list[InstanceSort] | None = None,
        cursors: dict[str, str | None] | None = None,
    ) -> Iterator[T_DomainModelList]:
        if cursors is not None and self._last_cursors is not None:
            raise ValueError(
                "Same cursors used twice. Please use a different set of cursors or start a new iteration. "
                "This is to avoid accidental infinite loops."
            )
        self._last_cursors = cursors
        executor = self._build(filter_, limit, retrieve_connections, sort, chunk_size)
        for batch_results in executor.iterate(self._client, remove_not_connected=False, init_cursors=cursors):
            unpacked = QueryUnpacker(batch_results, edges="skip").unpack()
            yield self._class_list(
                instantiate_classes(self._class_type, unpacked, "iterate"), cursors=batch_results._cursors
            )

    def _search(
        self,
        query: str,
        properties: str | SequenceNotStr[str] | None = None,
        filter_: dm.Filter | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        sort_by: str | list[str] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> T_DomainModelList:
        properties_input = self._to_input_properties(properties)

        sort_input = self._create_sort(sort_by, direction, sort)
        nodes = self._client.data_modeling.instances.search(
            view=self._view_id,
            query=query,
            instance_type="node",
            properties=properties_input,
            filter=filter_,
            limit=limit,
            sort=sort_input,
        )
        return self._class_list(
            instantiate_classes(self._class_type, [self._class_type._to_dict(node) for node in nodes], "search")
        )

    def _to_input_properties(self, properties: str | SequenceNotStr[str] | None) -> list[str] | None:
        properties_input: list[str] | None = None
        if isinstance(properties, str):
            properties_input = [properties]
        elif isinstance(properties, Sequence):
            properties_input = list(properties)
        if properties_input:
            properties_input = [self._properties_by_field.get(prop, prop) for prop in properties_input]
        return properties_input

    def _aggregate(
        self,
        aggregate: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation]
        ),
        group_by: str | SequenceNotStr[str] | None = None,
        properties: str | SequenceNotStr[str] | None = None,
        query: str | None = None,
        search_properties: str | SequenceNotStr[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        if isinstance(group_by, str):
            group_by = [group_by]

        if group_by:
            group_by = [self._properties_by_field.get(prop, prop) for prop in group_by]

        search_properties_input = self._to_input_properties(search_properties)

        if isinstance(properties, str):
            properties = [properties]

        if properties:
            properties = [self._properties_by_field.get(prop, prop) for prop in properties]

        is_single_aggregate = False
        if isinstance(aggregate, str | dm.aggregations.MetricAggregation):
            aggregate = [aggregate]
            is_single_aggregate = True

        if properties is None and (invalid := [agg for agg in aggregate if isinstance(agg, str) and agg != "count"]):
            raise ValueError(f"Cannot aggregate on {invalid} without specifying properties")

        aggregates: list[dm.aggregations.MetricAggregation] = []
        for agg in aggregate:
            if isinstance(agg, dm.aggregations.MetricAggregation):
                aggregates.append(agg)
            elif isinstance(agg, str):
                if agg == "count" and properties is None:
                    aggregates.append(dm.aggregations.Count("externalId"))
                elif properties is None:
                    raise ValueError(f"Cannot aggregate on {agg} without specifying properties")
                else:
                    for prop in properties:
                        aggregates.append(_METRIC_AGGREGATIONS_BY_NAME[agg](prop))
            else:
                raise TypeError(f"Expected str or MetricAggregation, got {type(agg)}")

        return self._client.data_modeling.instances.aggregate(
            view=self._view_id,
            aggregates=aggregates[0] if is_single_aggregate else aggregates,
            group_by=group_by,
            instance_type="node",
            query=query,
            properties=search_properties_input,
            filter=filter,
            limit=limit,
        )

    def _histogram(
        self,
        property: str,
        interval: float,
        query: str | None = None,
        search_properties: str | SequenceNotStr[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        property = self._properties_by_field.get(property, property)

        if isinstance(search_properties, str):
            search_properties = [search_properties]
        if search_properties:
            search_properties = [self._properties_by_field.get(prop, prop) for prop in search_properties]

        return self._client.data_modeling.instances.histogram(
            view=self._view_id,
            histograms=dm.aggregations.Histogram(property, interval),
            instance_type="node",
            query=query,
            properties=search_properties,
            filter=filter,
            limit=limit,
        )

    def _list(self, limit: int, filter: dm.Filter | None, sort: list[InstanceSort] | None = None) -> T_DomainModelList:
        nodes = self._client.data_modeling.instances.list(
            instance_type="node",
            sources=self._view_id,
            limit=limit,
            filter=filter,
            sort=sort,
        )
        return self._class_list(
            instantiate_classes(self._class_type, [self._class_type._to_dict(node) for node in nodes], "list")
        )

    def _create_sort(
        self,
        sort_by: str | list[str] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> list[InstanceSort] | None:
        sort_input: list[InstanceSort] | None = None
        if sort is None and isinstance(sort_by, str):
            sort_input = [self._create_sort_entry(sort_by, direction)]
        elif sort is None and isinstance(sort_by, list):
            sort_input = [self._create_sort_entry(sort_by_, direction) for sort_by_ in sort_by]
        elif sort is not None:
            sort_input = sort if isinstance(sort, list) else [sort]
            for sort_ in sort_input:
                if isinstance(sort_.property, Sequence) and len(sort_.property) == 1:
                    sort_.property = self._create_property_reference(sort_.property[0])
                elif isinstance(sort_.property, str):
                    sort_.property = self._create_property_reference(sort_.property)
        return sort_input

    def _create_sort_entry(self, sort_by: str, direction: Literal["ascending", "descending"]) -> InstanceSort:
        return InstanceSort(self._create_property_reference(sort_by), direction)

    def _create_property_reference(self, property_: str) -> list[str] | tuple[str, ...]:
        prop_name = self._properties_by_field.get(property_, property_)
        if prop_name in NODE_PROPERTIES:
            return ["node", prop_name]
        else:
            return self._view_id.as_property_ref(prop_name)


class NodeAPI(
    Generic[T_DomainModel, T_DomainModelWrite, T_DomainModelList, T_DomainModelWriteList],
    NodeReadAPI[T_DomainModel, T_DomainModelList],
    ABC,
):
    _class_write_list: type[T_DomainModelWriteList]

    def _apply(
        self, item: T_DomainModelWrite | Sequence[T_DomainModelWrite], replace: bool = False, write_none: bool = False
    ) -> ResourcesWriteResult:
        if isinstance(item, DomainModelWrite):
            instances = item.to_instances_write(write_none)
        else:
            instances = self._class_write_list(item).to_instances_write(write_none)
        result = self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )
        time_series = TimeSeriesList([])
        if instances.time_series:
            time_series = self._client.time_series.upsert(instances.time_series, mode="patch")

        return ResourcesWriteResult(result.nodes, result.edges, time_series)


class EdgeAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def _list(
        self,
        limit: int = DEFAULT_LIMIT_READ,
        filter_: dm.Filter | None = None,
    ) -> dm.EdgeList:
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=filter_)


class EdgePropertyAPI(EdgeAPI, Generic[T_DomainRelation, T_DomainRelationWrite, T_DomainRelationList], ABC):
    _view_id: ClassVar[dm.ViewId]
    _class_type: type[T_DomainRelation]
    _class_write_type: type[T_DomainRelationWrite]
    _class_list: type[T_DomainRelationList]

    def _list(  # type: ignore[override]
        self,
        limit: int = DEFAULT_LIMIT_READ,
        filter_: dm.Filter | None = None,
    ) -> T_DomainRelationList:
        edges = self._client.data_modeling.instances.list("edge", limit=limit, filter=filter_, sources=[self._view_id])
        return self._class_list(
            instantiate_classes(self._class_type, [self._class_type._to_dict(edge) for edge in edges], "list")
        )


class QueryAPI(Generic[T_DomainModel, T_DomainModelList]):
    def __init__(
        self,
        client: CogniteClient,
        builder: QueryBuilder,
        result_cls: type[T_DomainModel],
        result_list_cls: type[T_DomainModelList],
    ):
        self._client = client
        self._builder = builder
        self._result_cls = result_cls
        self._result_list_cls = result_list_cls

    def _query(self) -> T_DomainModelList:
        executor = self._builder.build()
        results = executor.execute_query(self._client, remove_not_connected=True)
        unpacked = QueryUnpacker(results).unpack()
        item_list = instantiate_classes(self._result_cls, unpacked, "query")
        return self._result_list_cls(item_list)


def _create_edge_filter(
    edge_type: dm.DirectRelationReference,
    start_node: dm.NodeId | list[dm.NodeId] | None = None,
    end_node: dm.NodeId | list[dm.NodeId] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter:
    filters: list[dm.Filter] = [
        dm.filters.Equals(
            ["edge", "type"],
            {"space": edge_type.space, "externalId": edge_type.external_id},
        )
    ]
    if start_node and isinstance(start_node, dm.NodeId):
        filters.append(
            dm.filters.Equals(
                ["edge", "startNode"], value=start_node.dump(camel_case=True, include_instance_type=False)
            )
        )
    if start_node and isinstance(start_node, list):
        filters.append(
            dm.filters.In(
                ["edge", "startNode"],
                values=[ext_id.dump(camel_case=True, include_instance_type=False) for ext_id in start_node],
            )
        )
    if end_node and isinstance(end_node, dm.NodeId):
        filters.append(
            dm.filters.Equals(["edge", "endNode"], value=end_node.dump(camel_case=True, include_instance_type=False))
        )
    if end_node and isinstance(end_node, list):
        filters.append(
            dm.filters.In(
                ["edge", "endNode"],
                values=[ext_id.dump(camel_case=True, include_instance_type=False) for ext_id in end_node],
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["edge", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["edge", "space"], value=space))
    if space and (space, list):
        filters.append(dm.filters.In(["edge", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters)


class GraphQLQueryResponse:
    def __init__(self, data_model_id: dm.DataModelId):
        self._output = GraphQLList([])
        self._data_class_by_type = _GRAPHQL_DATA_CLASS_BY_DATA_MODEL_BY_TYPE[data_model_id]

    def parse(self, response: dict[str, Any]) -> GraphQLList:
        if "errors" in response:
            raise RuntimeError(response["errors"])
        _, data = next(iter(response.items()))
        self._parse_item(data)
        if "pageInfo" in data:
            self._output.page_info = PageInfo.load(data["pageInfo"])
        return self._output

    def _parse_item(self, data: dict[str, Any]) -> None:
        if "items" in data:
            for item in data["items"]:
                self._parse_item(item)
        elif "__typename" in data:
            try:
                item = self._data_class_by_type[data["__typename"]].model_validate(data)
            except KeyError:
                raise ValueError(f"Could not find class for type {data['__typename']}") from None
            else:
                self._output.append(item)
        else:
            raise RuntimeError("Missing '__typename' in GraphQL response. Cannot determine the type of the response.")


_GRAPHQL_DATA_CLASS_BY_DATA_MODEL_BY_TYPE: dict[dm.DataModelId, dict[str, type[GraphQLCore]]] = {
    dm.DataModelId("sp_pygen_models", "OmniSub", "1"): {
        "ConnectionItemA": data_classes.ConnectionItemAGraphQL,
        "ConnectionItemB": data_classes.ConnectionItemBGraphQL,
        "ConnectionItemC": data_classes.ConnectionItemCNodeGraphQL,
    },
}


T_BaseModel = TypeVar("T_BaseModel", bound=BaseModel)


def instantiate_classes(cls_: type[T_BaseModel], data: list[dict[str, Any]], context: str) -> list[T_BaseModel]:
    if global_config.validate_retrieve is False:
        return [cls_.model_construct(**item) for item in data]

    cls_list = TypeAdapter(list[cls_])  # type: ignore[valid-type]
    try:
        return cls_list.validate_python(data)
    except ValidationError as e:
        failed_count = len({item["loc"][0] for item in e.errors()})
        msg = f"Failed to {context} {cls_.__name__!r}, {failed_count} out of {len(data)} instances failed validation."
        raise PygenValidationError(msg, e) from e


class PygenValidationError(ValueError):
    def __init__(self, message, pydantic_error: ValidationError) -> None:
        super().__init__(message)
        self.errors = pydantic_error.errors()

    def __str__(self):
        return (
            f"{super().__str__()}\nFor details see the ValidationError above."
            "\nHint: You can turn off validation by setting `global_config.validate_retrieve = False` by"
            f" importing `from omni_sub.config import global_config`."
        )
