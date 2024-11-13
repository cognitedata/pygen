import copy
import itertools
import warnings
from collections import defaultdict
from collections.abc import Callable, Sequence
from functools import cached_property
from typing import Any, Literal, TypeAlias, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import filters
from cognite.client.data_classes.aggregations import Aggregation
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList
from cognite.client.data_classes.data_modeling.views import ReverseDirectRelation, ViewProperty
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils.useful_types import SequenceNotStr

from cognite.pygen._version import __version__

from .query_builder import (
    AGGREGATION_LIMIT,
    IN_FILTER_CHUNK_SIZE,
    SEARCH_LIMIT,
    QueryBuilder,
    QueryStep,
    ViewPropertyId,
    chunker,
)

_NODE_PROPERTIES = frozenset(
    {"externalId", "space", "version", "lastUpdatedTime", "createdTime", "deletedTime", "type"}
)

Properties: TypeAlias = list[str | dict[str, list[str | dict[str, Any]]]]


class QueryExecutor:
    def __init__(self, client: CogniteClient, views: Sequence[dm.View] | None = None):
        self._client = client
        # Used for aggregated logging of requests
        client.config.client_name = f"CognitePygen:{__version__}:QueryExecutor"
        self._view_by_id: dict[dm.ViewId, dm.View] = {view.as_id(): view for view in views or []}

    def _get_view(self, view_id: dm.ViewId) -> dm.View:
        if view_id not in self._view_by_id:
            view = self._client.data_modeling.views.retrieve(view_id, all_versions=False)
            if not view:
                raise CogniteAPIError(f"View not found: {view_id!r}", code=200)
            self._view_by_id[view_id] = view[0]
        return self._view_by_id[view_id]

    @staticmethod
    def _as_property_list(properties: Properties, operation: str) -> list[str]:
        output = []
        is_nested_supported = operation == "list"
        for prop in properties:
            if isinstance(prop, str):
                output.append(prop)
            elif isinstance(prop, dict) and is_nested_supported:
                if len(prop) != 1:
                    raise ValueError(f"Unexpected nested property: {prop}")
                key = next(iter(prop.keys()))
                output.append(key)
            elif isinstance(prop, dict):
                raise ValueError(f"Nested properties are not supported for operation {operation}")
            else:
                raise ValueError(f"Unexpected property type: {type(prop)}")
        return output

    def execute_query(
        self,
        view: dm.ViewId,
        operation: Literal["list", "aggregate", "search"],
        properties: Properties | None = None,
        filter: filters.Filter | None = None,
        query: str | None = None,
        group_by: str | SequenceNotStr[str] | None = None,
        aggregates: Aggregation | Sequence[Aggregation] | None = None,
        sort: dm.InstanceSort | Sequence[dm.InstanceSort] | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        warnings.warn(
            "This method is deprecated. Use list, aggregate or search methods instead", UserWarning, stacklevel=2
        )
        dumped: Any
        if operation == "list":
            if properties is None:
                raise ValueError("Properties are required for list operation")
            dumped = self._execute_list(view, properties, filter, sort, limit)
        elif operation == "aggregate":
            if not aggregates:
                raise ValueError("Aggregates are required for aggregate operation")
            flatten_props = self._as_property_list(properties, operation) if properties else None
            dumped = self._execute_aggregation(view, aggregates, flatten_props, query, filter, group_by, limit)
        elif operation == "search":
            flatten_props = self._as_property_list(properties, operation) if properties else None
            search_result = self._client.data_modeling.instances.search(
                view, query, properties=flatten_props, filter=filter, limit=limit or SEARCH_LIMIT, sort=sort
            )
            dumped = self._prepare_list_result(search_result, set(flatten_props) if flatten_props else None)
        else:
            raise NotImplementedError(f"Operation {operation} is not supported")
        return {f"{operation}{view.external_id}": dumped}

    def _execute_list(
        self,
        view_id: dm.ViewId,
        properties: Properties,
        filter: filters.Filter | None = None,
        sort: Sequence[dm.InstanceSort] | dm.InstanceSort | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        view = self._get_view(view_id)
        root_properties = self._as_property_list(properties, "list")
        builder = QueryBuilder()
        factory = QueryStepFactory(view, properties, builder.create_name)

        if not factory.connection_properties:
            result = self._client.data_modeling.instances.list(
                instance_type="node",
                sources=[view_id],
                filter=filter,
                limit=limit,
                sort=sort,
            )
            return self._prepare_list_result(result, set(root_properties))

        reverse_views = {
            prop.through.source: self._get_view(prop.through.source)
            for prop in factory.reverse_properties.values()
            if isinstance(prop.through.source, dm.ViewId)
        }
        builder.append(factory.root(filter, limit))
        for connection_id, connection in factory.connection_properties.items():
            builder.extend(factory.from_connection(connection_id, connection, reverse_views))
        _ = builder.execute_query(self._client, remove_not_connected=False)
        return QueryUnpacker(builder).unpack()

    @classmethod
    def _prepare_list_result(
        cls, result: dm.NodeList[dm.Node], selected_properties: set[str] | None
    ) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        for node in result:
            item = QueryUnpacker.flatten_dump(node, selected_properties)
            if item:
                output.append(item)
        return output

    def _execute_aggregation(
        self,
        view_id: dm.ViewId,
        aggregates: Aggregation | Sequence[Aggregation],
        search_properties: str | SequenceNotStr[str] | None = None,
        query: str | None = None,
        filter: filters.Filter | None = None,
        group_by: str | SequenceNotStr[str] | None = None,
        limit: int | None = None,
    ) -> dict[str, Any] | list[dict[str, Any]]:
        aggregates_list = aggregates if isinstance(aggregates, Sequence) else [aggregates]
        metric_aggregates = [agg for agg in aggregates_list if isinstance(agg, dm.aggregations.MetricAggregation)]
        histogram_aggregates = [agg for agg in aggregates_list if isinstance(agg, dm.aggregations.Histogram)]
        if metric_aggregates and histogram_aggregates:
            raise ValueError("Cannot mix metric and histogram aggregations")

        if metric_aggregates and group_by is not None:
            group_by_result = self._client.data_modeling.instances.aggregate(  # type: ignore[call-overload]
                view=view_id,
                group_by=group_by,
                aggregates=metric_aggregates,
                query=query,
                properties=search_properties,
                filter=filter,
                limit=limit or AGGREGATION_LIMIT,
            )
            return self._grouped_metric_aggregation_to_dict(group_by_result)
        elif metric_aggregates:
            metric_results = self._client.data_modeling.instances.aggregate(
                view_id,
                aggregates=metric_aggregates,
                query=query,
                properties=search_properties,
                filter=filter,
                limit=limit or AGGREGATION_LIMIT,
            )
            return self._metric_aggregation_to_dict(metric_results)

        elif histogram_aggregates:
            histogram_results = self._client.data_modeling.instances.histogram(
                view_id,
                histograms=histogram_aggregates,
                query=query,
                properties=search_properties,  # type: ignore[arg-type]
                filter=filter,
                limit=limit or AGGREGATION_LIMIT,
            )
            return self._histogram_aggregation_to_dict(histogram_results)
        else:
            raise ValueError("No aggregation found")

    @staticmethod
    def _metric_aggregation_to_dict(aggregation: list[dm.aggregations.AggregatedNumberedValue]) -> dict[str, Any]:
        values_by_aggregations: dict[str, dict[str, Any]] = defaultdict(dict)
        for item in aggregation:
            values_by_aggregations[item._aggregate][item.property] = item.value
        return dict(values_by_aggregations)

    @classmethod
    def _grouped_metric_aggregation_to_dict(cls, aggregations: InstanceAggregationResultList) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        for group in aggregations:
            group_dict = {
                "group": group.group,
                **cls._metric_aggregation_to_dict(group.aggregates),
            }
            output.append(group_dict)
        return output

    @staticmethod
    def _histogram_aggregation_to_dict(aggregation: list[dm.aggregations.HistogramValue]) -> dict[str, Any]:
        output: dict[str, dict[str, Any]] = defaultdict(dict)
        for item in aggregation:
            output[item._aggregate][item.property] = {
                "interval": item.interval,
                "buckets": [bucket.dump() for bucket in item.buckets],
            }
        return dict(output)

    def search(
        self,
        view: dm.ViewId,
        properties: Properties | None = None,
        query: str | None = None,
        filter: filters.Filter | None = None,
        search_properties: str | SequenceNotStr[str] | None = None,
        sort: Sequence[dm.InstanceSort] | dm.InstanceSort | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        search_result = self._client.data_modeling.instances.search(
            view,
            query,
            properties=search_properties,  # type: ignore[arg-type]
            filter=filter,
            limit=limit or SEARCH_LIMIT,
            sort=sort,
        )

        flatten_props = self._as_property_list(properties, "list") if properties else None
        are_flat_properties = flatten_props == properties
        if properties is None or are_flat_properties:
            return self._prepare_list_result(search_result, set(flatten_props) if flatten_props else None)

        # Lookup nested properties:

        order_by_node_ids = {node.as_id(): no for no, node in enumerate(search_result)}
        # If we are sorting, then we need to ensure externalId and space are included in the properties.
        # This is because we need them for the final sorting.
        include_space = False
        include_external_id = False
        if sort is not None:
            include_space = "space" not in properties
            include_external_id = "externalId" not in properties
        if include_space:
            properties.append("space")
        if include_external_id:
            properties.append("externalId")

        result: list[dict[str, Any]] = []
        for space, space_nodes in itertools.groupby(
            sorted(order_by_node_ids.keys(), key=lambda x: x.space), key=lambda x: x.space
        ):
            is_space = filters.Equals(["node", "space"], space)
            for chunk in chunker(list(space_nodes), IN_FILTER_CHUNK_SIZE):
                batch_filter = filters.And(
                    filters.In(["node", "externalId"], [node.external_id for node in chunk]), is_space
                )
                batch_result = self.list(view, properties, batch_filter, sort, limit or SEARCH_LIMIT)
                result.extend(batch_result)

        if sort is not None:
            result.sort(key=lambda x: order_by_node_ids[dm.NodeId(x["space"], x["externalId"])])
        if include_space or include_external_id:
            for item in result:
                if include_space:
                    del item["space"]
                if include_external_id:
                    del item["externalId"]

        return result

    @overload
    def aggregate(
        self,
        view: dm.ViewId,
        aggregates: Aggregation | Sequence[Aggregation],
        group_by: None = None,
        filter: filters.Filter | None = None,
        query: str | None = None,
        search_properties: str | SequenceNotStr[str] | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]: ...

    @overload
    def aggregate(
        self,
        view: dm.ViewId,
        aggregates: Aggregation | Sequence[Aggregation],
        group_by: str | SequenceNotStr[str],
        filter: filters.Filter | None = None,
        query: str | None = None,
        search_properties: str | SequenceNotStr[str] | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]: ...

    def aggregate(
        self,
        view: dm.ViewId,
        aggregates: Aggregation | Sequence[Aggregation],
        group_by: str | SequenceNotStr[str] | None = None,
        filter: filters.Filter | None = None,
        query: str | None = None,
        search_properties: str | SequenceNotStr[str] | None = None,
        limit: int | None = None,
    ) -> dict[str, Any] | list[dict[str, Any]]:
        return self._execute_aggregation(view, aggregates, search_properties, query, filter, group_by, limit)

    def list(
        self,
        view: dm.ViewId,
        properties: Properties,
        filter: filters.Filter | None = None,
        sort: Sequence[dm.InstanceSort] | dm.InstanceSort | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        return self._execute_list(view, properties, filter, sort, limit)


class QueryStepFactory:
    def __init__(
        self,
        view: dm.View,
        user_selected_properties: Properties,
        create_step_name: Callable[[str | None], str],
    ) -> None:
        self._view = view
        self._view_id = view.as_id()
        self._user_selected_properties = user_selected_properties
        self._create_step_name = create_step_name
        self._root_name: str | None = None

    @cached_property
    def _root_properties(self) -> list[str]:
        root_properties: list[str] = []
        for prop in self._user_selected_properties:
            if isinstance(prop, str):
                root_properties.append(prop)
            elif isinstance(prop, dict):
                key = next(iter(prop.keys()))
                root_properties.append(key)
        return root_properties

    @cached_property
    def connection_properties(self) -> dict[str, ViewProperty]:
        output: dict[str, ViewProperty] = {}
        for prop in self._root_properties:
            definition = self._view.properties.get(prop)
            if not definition:
                continue
            if self._is_connection(definition):
                output[prop] = definition
        return output

    @cached_property
    def reverse_properties(self) -> dict[str, ReverseDirectRelation]:
        return {
            prop_id: prop
            for prop_id, prop in self.connection_properties.items()
            if isinstance(prop, ReverseDirectRelation)
        }

    @cached_property
    def _nested_properties_by_property(self) -> dict[str, list[str | dict[str, list[str]]]]:
        nested_properties_by_property: dict[str, list[str | dict[str, list[str]]]] = {}
        for prop in self._user_selected_properties:
            if isinstance(prop, dict):
                key, value = next(iter(prop.items()))
                nested_properties_by_property[key] = value
        return nested_properties_by_property

    def root(self, filter: dm.Filter | None = None, limit: int | None = None) -> QueryStep:
        skip = _NODE_PROPERTIES | set(self.reverse_properties.keys())
        if self._root_name is not None:
            raise ValueError("Root step is already created")
        self._root_name = self._create_step_name(None)
        has_data = dm.filters.HasData(views=[self._view_id])
        return QueryStep(
            self._root_name,
            dm.query.NodeResultSetExpression(
                filter=dm.filters.And(filter, has_data) if filter else has_data,
            ),
            select=self._create_select([prop for prop in self._root_properties if prop not in skip], self._view_id),
            selected_properties=self._root_properties,
            view_id=self._view_id,
            max_retrieve_limit=-1 if limit is None else limit,
            raw_filter=filter,
        )

    def from_connection(
        self, connection_id: str, connection: ViewProperty, reverse_views: dict[dm.ViewId, dm.View]
    ) -> list[QueryStep]:
        if self._root_name is None:
            raise ValueError("Root step is not created")
        view_property = ViewPropertyId(self._view_id, connection_id)
        selected_properties = self._nested_properties_by_property.get(connection_id, ["*"])
        if isinstance(connection, dm.EdgeConnection):
            return self._from_edge(connection, view_property, selected_properties)
        elif isinstance(connection, ReverseDirectRelation):
            validated = self._validate_flat_properties(selected_properties)
            return self._from_reverse_relation(connection, view_property, validated, reverse_views)
        elif isinstance(connection, dm.MappedProperty) and isinstance(connection.type, dm.DirectRelation):
            validated = self._validate_flat_properties(selected_properties)
            return self.from_direct_relation(connection, view_property, validated)
        else:
            warnings.warn(f"Unexpected connection type: {connection!r}", UserWarning, stacklevel=2)
        return []

    def from_direct_relation(
        self,
        connection: dm.MappedProperty,
        view_property: ViewPropertyId,
        selected_properties: list[str],
    ) -> list[QueryStep]:
        query_properties = self._create_query_properties(selected_properties, None)
        if connection.source is None:
            raise ValueError("Source view not found")
        return [
            QueryStep(
                self._create_step_name(self._root_name),
                dm.query.NodeResultSetExpression(
                    from_=self._root_name,
                    direction="outwards",
                    through=self._view_id.as_property_ref(view_property.property),
                ),
                view_property=view_property,
                select=self._create_select(query_properties, connection.source),
                selected_properties=selected_properties,
                view_id=connection.source,
            )
        ]

    def _from_edge(
        self,
        connection: dm.EdgeConnection,
        view_property: ViewPropertyId,
        selected_properties: list[str | dict[str, list[str]]],
    ) -> list[QueryStep]:
        edge_name = self._create_step_name(self._root_name)
        steps = []
        step = QueryStep(
            edge_name,
            dm.query.EdgeResultSetExpression(
                from_=self._root_name,
                direction=connection.direction,
                chain_to="source" if connection.direction == "outwards" else "destination",
            ),
            selected_properties=[prop for prop in selected_properties if isinstance(prop, str)],
            view_property=view_property,
        )
        steps.append(step)

        node_properties = next(
            (prop for prop in selected_properties if isinstance(prop, dict) and "node" in prop), None
        )
        if isinstance(node_properties, dict):
            selected_node_properties = node_properties["node"]
            query_properties = self._create_query_properties(selected_node_properties, None)
            target_view = connection.source

            step = QueryStep(
                self._create_step_name(edge_name),
                dm.query.NodeResultSetExpression(
                    from_=edge_name,
                    filter=dm.filters.HasData(views=[target_view]),
                ),
                select=self._create_select(query_properties, target_view),
                selected_properties=selected_node_properties,
                view_property=ViewPropertyId(target_view, "node"),
                view_id=target_view,
            )
            steps.append(step)
        return steps

    def _from_reverse_relation(
        self,
        connection: ReverseDirectRelation,
        view_property: ViewPropertyId,
        selected_properties: list[str],
        reverse_views: dict[dm.ViewId, dm.View],
    ) -> list[QueryStep]:
        connection_type: Literal["reverse-list"] | None = (
            "reverse-list" if self._is_listable(connection.through, reverse_views) else None
        )
        query_properties = self._create_query_properties(selected_properties, connection.through.property)
        try:
            other_view = reverse_views[connection.source]
        except KeyError:
            raise ValueError(f"View {connection.source} not found in {reverse_views.keys()}") from None
        return [
            QueryStep(
                self._create_step_name(self._root_name),
                dm.query.NodeResultSetExpression(
                    from_=self._root_name,
                    direction="inwards",
                    through=other_view.as_property_ref(connection.through.property),
                ),
                view_property=view_property,
                select=self._create_select(query_properties, other_view.as_id()),
                selected_properties=selected_properties,
                view_id=connection.source,
                connection_type=connection_type,
            )
        ]

    @classmethod
    def _create_query_properties(cls, properties: list[str], connection_id: str | None = None) -> list[str]:
        include_connection_prop = "*" not in properties
        nested_properties: list[str] = []
        for prop_id in properties:
            if prop_id in _NODE_PROPERTIES:
                continue
            if prop_id == connection_id:
                include_connection_prop = False
            nested_properties.append(prop_id)
        if include_connection_prop and connection_id:
            nested_properties.append(connection_id)
        return nested_properties

    @staticmethod
    def _is_connection(definition: ViewProperty) -> bool:
        return isinstance(definition, dm.ConnectionDefinition) or (
            isinstance(definition, dm.MappedProperty) and isinstance(definition.type, dm.DirectRelation)
        )

    @staticmethod
    def _create_select(properties: list[str], view_id: dm.ViewId) -> dm.query.Select:
        return dm.query.Select([dm.query.SourceSelector(view_id, properties)])

    @staticmethod
    def _is_listable(property: dm.PropertyId, reverse_views: dict[dm.ViewId, dm.View]) -> bool:
        if isinstance(property.source, dm.ViewId):
            try:
                view = reverse_views[property.source]
            except KeyError:
                raise ValueError(f"View {property.source} not found in {reverse_views.keys()}") from None
            if property.property not in view.properties:
                raise TypeError(f"Reverse property {property.property} not found in {property.source!r}")
            reverse_prop = view.properties[property.property]
            if not (isinstance(reverse_prop, dm.MappedProperty) and isinstance(reverse_prop.type, dm.DirectRelation)):
                raise TypeError(f"Property {property.property} is not a direct relation")
            return reverse_prop.type.is_list
        else:
            raise NotImplementedError(f"Property {property.source=} is not supported")

    @staticmethod
    def _validate_flat_properties(properties: list[str | dict[str, list[str]]]) -> list[str]:
        output = []
        for prop in properties:
            if isinstance(prop, str):
                output.append(prop)
            else:
                raise ValueError(f"Direct relations do not support nested properties. Got {prop}")
        return output


class QueryUnpacker:
    def __init__(self, builder: QueryBuilder):
        self._builder = builder

    def unpack(self) -> list[dict[str, Any]]:
        nodes_by_from: dict[str | None, list[tuple[str, dict[dm.NodeId, list[dict[str, Any]]]]]] = defaultdict(list)
        for step in reversed(self._builder):
            source_property = step.view_property and step.view_property.property
            if node_expression := step.node_expression:
                unpacked = self._unpack_node(step, node_expression, nodes_by_from)
                nodes_by_from[step.from_].append((source_property, unpacked))
            elif edge_expression := step.edge_expression:
                step_properties = set(step.selected_properties or [])
                unpacked_edge: dict[dm.NodeId, list[dict[str, Any]]] = defaultdict(list)
                for edge in step.edge_results:
                    start_node = dm.NodeId.load(edge.start_node.dump())
                    end_node = dm.NodeId.load(edge.end_node.dump())
                    dumped = self.flatten_dump(edge, step_properties)
                    if edge_expression.direction == "outwards":
                        source_node = start_node
                        target_node = end_node
                    else:
                        source_node = end_node
                        target_node = start_node
                    for nested_prop, nested_by_id in nodes_by_from.get(step.name, []):
                        if target_node in nested_by_id:
                            dumped[nested_prop] = nested_by_id[target_node]

                    unpacked_edge[source_node].append(dumped)
                nodes_by_from[step.from_].append((source_property, unpacked_edge))
            else:
                raise TypeError("Unexpected step")
        return [item[0] for item in nodes_by_from[None][0][1].values()]

    @classmethod
    def flatten_dump(
        cls, node: dm.Node | dm.Edge, selected_properties: set[str] | None, connection_property: str | None = None
    ) -> dict[str, Any]:
        dumped = node.dump()
        dumped_properties = dumped.pop("properties", {})
        item = {
            key: value for key, value in dumped.items() if selected_properties is None or key in selected_properties
        }
        for _, props_by_view_id in dumped_properties.items():
            for __, props in props_by_view_id.items():
                for key, value in props.items():
                    if key == connection_property:
                        if isinstance(value, dict):
                            item[key] = dm.NodeId.load(value)
                        elif isinstance(value, list):
                            item[key] = [dm.NodeId.load(item) for item in value]
                        else:
                            raise TypeError(f"Unexpected connection property value: {value}")
                    elif selected_properties is None or key in selected_properties:
                        item[key] = value
        return item

    def _unpack_node(
        self,
        step: QueryStep,
        node_expression: dm.query.NodeResultSetExpression,
        results_by_from: dict[str | None, list[tuple[str, dict[dm.NodeId, list[dict[str, Any]]]]]],
    ) -> dict[dm.NodeId, list[dict[str, Any]]]:
        step_properties = set(step.selected_properties or [])
        connection_property: str | None = None
        if node_expression.through and node_expression.direction == "inwards":
            connection_property = node_expression.through.property
        unpacked: dict[dm.NodeId, list[dict[str, Any]]] = defaultdict(list)
        for node in step.node_results:
            node_id = node.as_id()
            dumped = self.flatten_dump(node, step_properties, connection_property)
            for nested_prop, nested_by_id in results_by_from.get(step.name, []):
                if neste_item := nested_by_id.get(node_id):
                    # Reverse or Edge
                    dumped[nested_prop] = neste_item
                elif nested_prop in dumped:
                    # Direct relation
                    identifier = dumped.pop(nested_prop)
                    if isinstance(identifier, dict):
                        other_id = dm.NodeId.load(identifier)
                        if other_id in nested_by_id:
                            dumped[nested_prop] = nested_by_id[other_id]
                    elif isinstance(identifier, list):
                        dumped[nested_prop] = []
                        for item in identifier:
                            other_id = dm.NodeId.load(item)
                            if other_id in nested_by_id:
                                dumped[nested_prop].extend(nested_by_id[other_id])
                else:
                    warnings.warn(
                        f"Nested property {nested_prop} not found in {dumped.keys()}", UserWarning, stacklevel=2
                    )

            if connection_property is None:
                unpacked[node_id].append(dumped)
            else:
                reverse = dumped.pop(connection_property)
                if isinstance(reverse, dm.NodeId):
                    unpacked[reverse].append(dumped)
                elif isinstance(reverse, list):
                    for item in reverse:
                        unpacked[item].append(copy.deepcopy(dumped))
        return unpacked
