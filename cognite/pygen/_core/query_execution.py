import copy
from collections import defaultdict
from collections.abc import Sequence
from typing import Any, Literal

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import filters
from cognite.client.data_classes.aggregations import MetricAggregation
from cognite.client.data_classes.data_modeling.views import ReverseDirectRelation, ViewProperty
from cognite.client.exceptions import CogniteAPIError

from .query_builder import SEARCH_LIMIT, QueryBuilder, QueryStep, ViewPropertyId


class QueryExecutor:
    def __init__(self, client: CogniteClient, views: Sequence[dm.View] | None = None):
        self._client = client
        self._view_by_id: dict[dm.ViewId, dm.View] = {view.as_id(): view for view in views or []}

    def _get_view(self, view_id: dm.ViewId) -> dm.View:
        if view_id not in self._view_by_id:
            view = self._client.data_modeling.views.retrieve(view_id, all_versions=False)
            if not view:
                raise CogniteAPIError(f"View not found: {view_id!r}", code=200)
            self._view_by_id[view_id] = view[0]
        return self._view_by_id[view_id]

    def _flatten_properties(self, properties: list[str | dict[str, list[str]]], operation: str) -> list[str]:
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
        properties: list[str | dict[str, list[str]]],
        filter: filters.Filter | None = None,
        query: str | None = None,
        groupby: str | Sequence[str] | None = None,
        aggregates: MetricAggregation | Sequence[MetricAggregation] | None = None,
        sort: dm.InstanceSort | Sequence[dm.InstanceSort] | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        if operation == "list":
            dumped = self._execute_list(view, properties, filter, sort, limit)
        elif operation == "aggregate":
            aggregate_result = self._client.data_modeling.instances.aggregate(  # type: ignore[misc]
                view,
                aggregates=aggregates,  # type: ignore[arg-type]
                group_by=groupby,  # type: ignore[arg-type]
                query=query,
                properties=self._flatten_properties(properties, operation),
                filter=filter,
                limit=limit,  # type: ignore[arg-type]
            )
            dumped = self._prepare_aggregate_result(aggregate_result)
        elif operation == "search":
            flatten_props = self._flatten_properties(properties, operation)
            search_result = self._client.data_modeling.instances.search(
                view, query, properties=flatten_props, filter=filter, limit=limit or SEARCH_LIMIT, sort=sort
            )
            dumped = self._prepare_list_result(search_result, set(flatten_props))
        else:
            raise NotImplementedError(f"Operation {operation} is not supported")
        return {f"{operation}{view.external_id}": dumped}

    def _execute_list(
        self,
        view_id: dm.ViewId,
        properties: list[str | dict[str, list[str]]],
        filter: filters.Filter | None = None,
        sort: Sequence[dm.InstanceSort] | dm.InstanceSort | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        view = self._get_view(view_id)
        flatten_properties = self._flatten_properties(properties, "list")
        connection_properties = self._get_connection_properties(view.properties, flatten_properties)

        if not connection_properties:
            result = self._client.data_modeling.instances.list(
                instance_type="node",
                sources=[view_id],
                filter=filter,
                limit=limit,
                sort=sort,
            )
            return self._prepare_list_result(result, set(flatten_properties))

        nested_properties_by_property = self._as_nested_property_by_properties(properties)

        builder = QueryBuilder()
        has_data = dm.filters.HasData(views=[view_id])
        root_name = builder.create_name(None)
        builder.append(
            QueryStep(
                root_name,
                dm.query.NodeResultSetExpression(
                    filter=dm.filters.And(filter, has_data) if filter else has_data,
                ),
                select=self._create_select(
                    [prop for prop in flatten_properties if prop not in nested_properties_by_property], view_id
                ),
                view_id=view.as_id(),
                max_retrieve_limit=-1 if limit is None else limit,
                raw_filter=filter,
            )
        )
        for connection_id, connection in connection_properties.items():
            view_property = ViewPropertyId(view_id, connection_id)
            if isinstance(connection, dm.EdgeConnection):
                edge_name = builder.create_name(root_name)
                builder.append(
                    QueryStep(
                        edge_name,
                        dm.query.EdgeResultSetExpression(
                            from_=root_name,
                            direction=connection.direction,
                            chain_to="source" if connection.direction == "outwards" else "destination",
                        ),
                    )
                )
                target_view = connection.source
                builder.append(
                    QueryStep(
                        builder.create_name(edge_name),
                        dm.query.NodeResultSetExpression(
                            from_=edge_name,
                            filter=dm.filters.HasData(views=[target_view]),
                        ),
                        view_property=view_property,
                        view_id=target_view,
                    )
                )
            elif (
                isinstance(connection, dm.MappedProperty)
                and isinstance(connection.type, dm.DirectRelation)
                and connection.source
            ):
                builder.append(
                    QueryStep(
                        builder.create_name(root_name),
                        dm.query.NodeResultSetExpression(
                            from_=root_name,
                            direction="outwards",
                            through=view_id.as_property_ref(connection_id),
                        ),
                        view_property=view_property,
                        select=self._create_select(
                            nested_properties_by_property.get(connection_id, ["*"]), connection.source
                        ),
                        view_id=connection.source,
                    )
                )
            elif isinstance(connection, ReverseDirectRelation):
                connection_type: Literal["reverse-list"] | None = (
                    "reverse-list" if self._is_listable(connection.through) else None
                )
                other_view = self._get_view(connection.source)
                builder.append(
                    QueryStep(
                        builder.create_name(root_name),
                        dm.query.NodeResultSetExpression(
                            from_=root_name,
                            direction="inwards",
                            through=other_view.as_property_ref(connection.through.property),
                        ),
                        view_property=view_property,
                        select=self._create_select(
                            nested_properties_by_property.get(connection_id, ["*"]), other_view.as_id()
                        ),
                        view_id=connection.source,
                        connection_type=connection_type,
                    )
                )
        _ = builder.execute_query(self._client, remove_not_connected=False)
        return self._prepare_query_result(builder)

    @staticmethod
    def _as_nested_property_by_properties(properties: list[str | dict[str, list[str]]]) -> dict[str, list[str]]:
        nested_properties_by_property: dict[str, list[str]] = {}
        for prop in properties:
            if isinstance(prop, dict):
                key, value = next(iter(prop.items()))
                nested_properties_by_property[key] = value
        return nested_properties_by_property

    @classmethod
    def _get_connection_properties(
        cls, view_properties: dict[str, ViewProperty], properties: list[str]
    ) -> dict[str, ViewProperty]:
        output: dict[str, ViewProperty] = {}
        for prop in properties:
            definition = view_properties.get(prop)
            if not definition:
                continue
            if cls._is_connection(definition):
                output[prop] = definition
        return output

    @staticmethod
    def _is_connection(definition: ViewProperty) -> bool:
        return isinstance(definition, dm.ConnectionDefinition) or (
            isinstance(definition, dm.MappedProperty) and isinstance(definition.type, dm.DirectRelation)
        )

    @staticmethod
    def _create_select(properties: list[str], view_id: dm.ViewId) -> dm.query.Select:
        return dm.query.Select([dm.query.SourceSelector(view_id, properties)])

    def _is_listable(self, property: dm.PropertyId) -> bool:
        if isinstance(property.source, dm.ViewId):
            view = self._get_view(property.source)
            if property.property not in view.properties:
                raise TypeError(f"Reverse property {property.property} not found in {property.source!r}")
            reverse_prop = view.properties[property.property]
            if not (isinstance(reverse_prop, dm.MappedProperty) and isinstance(reverse_prop.type, dm.DirectRelation)):
                raise TypeError(f"Property {property.property} is not a direct relation")
            return reverse_prop.type.is_list
        else:
            raise NotImplementedError(f"Property {property.source=} is not supported")

    @classmethod
    def _prepare_list_result(cls, result: dm.NodeList[dm.Node], selected_properties: set[str]) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        for node in result:
            item = cls._flatten_dump(node, selected_properties)
            if item:
                output.append(item)
        return output

    @classmethod
    def _flatten_dump(
        cls, node: dm.Node, selected_properties: set[str], connection_property: str | None = None
    ) -> dict[str, Any]:
        dumped = node.dump()
        dumped_properties = dumped.pop("properties", {})
        item = {key: value for key, value in dumped.items() if key in selected_properties}
        for _, props_by_view_id in dumped_properties.items():
            for __, props in props_by_view_id.items():
                for key, value in props.items():
                    if key in selected_properties:
                        item[key] = value
                    elif key == connection_property:
                        if isinstance(value, dict):
                            item[key] = dm.NodeId.load(value)
                        elif isinstance(value, list):
                            item[key] = [dm.NodeId.load(item) for item in value]
                        else:
                            raise TypeError(f"Unexpected connection property value: {value}")
        return item

    def _prepare_query_result(self, builder: QueryBuilder) -> list[dict[str, Any]]:
        results_by_from: dict[str | None, list[tuple[str | None, dict[dm.NodeId, list[dict[str, Any]]]]]] = defaultdict(
            list
        )
        for step in reversed(builder):
            if node_expression := step.node_expression:
                unpacked = self._unpack_node(step, node_expression, results_by_from)

                source_property = step.view_property and step.view_property.property
                results_by_from[step.from_].append((source_property, unpacked))
            elif _ := step.edge_expression:
                raise NotImplementedError()
            else:
                raise TypeError("Unexpected step")
        return [item[0] for item in results_by_from[None][0][1].values()]

    def _unpack_node(
        self,
        step: QueryStep,
        node_expression: dm.query.NodeResultSetExpression,
        results_by_from: dict[str | None, list[tuple[str | None, dict[dm.NodeId, list[dict[str, Any]]]]]],
    ) -> dict[dm.NodeId, list[dict[str, Any]]]:
        step_properties = set(step.selected_properties)
        connection_property: str | None = None
        if node_expression.through and node_expression.direction == "inwards":
            connection_property = node_expression.through.property
        unpacked: dict[dm.NodeId, list[dict[str, Any]]] = defaultdict(list)
        for node in step.node_results:
            node_id = node.as_id()
            dumped = self._flatten_dump(node, step_properties, connection_property)
            if step.name in results_by_from:
                for nested_prop, nested_by_id in results_by_from[step.name]:
                    if nested_prop is None:
                        # Edge
                        raise NotImplementedError()
                    else:
                        if neste_item := nested_by_id.get(node_id):
                            dumped[nested_prop] = neste_item

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

    def _prepare_aggregate_result(self, result: Any) -> list[dict[str, Any]]:
        raise NotImplementedError()
