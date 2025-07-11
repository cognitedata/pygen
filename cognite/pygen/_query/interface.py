import itertools
import json
from collections import defaultdict
from collections.abc import Sequence
from typing import Any, Literal, cast, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import filters
from cognite.client.data_classes.aggregations import Aggregation, Avg
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils.useful_types import SequenceNotStr

from cognite.pygen._version import __version__

from .builder import QueryBuilder
from .constants import AGGREGATION_LIMIT, IN_FILTER_CHUNK_SIZE, SEARCH_LIMIT, SelectedProperties
from .executor import chunker
from .processing import QueryUnpacker
from .step import QueryBuildStepFactory


class QueryExecutor:
    """Class for executing queries against the Domain Model Storage (DMS) endpoints in CDF.

    Args:
        client (CogniteClient): An instance of the CogniteClient.
        views (Sequence[dm.View], optional): A list of views to use for the queries. Defaults to None.
            If not passed, the views will be fetched from the server when needed.
        unpack_edges (Literal["skip", "include"], optional): Whether to unpack edges in the result.
            If "skip", edges will not be included in the result. If "include", edges will be included.
            Defaults to "include".
    """

    def __init__(
        self,
        client: CogniteClient,
        views: Sequence[dm.View] | None = None,
        unpack_edges: Literal["skip", "include"] = "skip",
    ):
        self._client = client
        # Used for aggregated logging of requests
        if not client.config.client_name.startswith("CognitePygen"):
            client.config.client_name = f"CognitePygen:{__version__}:QueryExecutor:{client.config.client_name}"
        self._view_by_id: dict[dm.ViewId, dm.View] = {view.as_id(): view for view in views or []}
        self._unpack_edges: Literal["skip", "include"] = unpack_edges

    def search(
        self,
        view: dm.ViewId,
        properties: SelectedProperties | None = None,
        query: str | None = None,
        filter: filters.Filter | None = None,
        search_properties: str | SequenceNotStr[str] | None = None,
        sort: Sequence[dm.InstanceSort] | dm.InstanceSort | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Search for nodes/edges in a view.

        Note if the view supports both nodes and edges the result will be a list of nodes and edges.

        Nested properties are not supported for edges.

        Args:
            view: The view in which the nodes/edges have properties.
            properties: The properties to in include in the result. If None, all properties are included.
            query: The search query.
            filter: The filter to apply ahead of the search.
            search_properties: The properties to search. If None, all text properties are searched.
            sort: The sort order of the results.
            limit: The maximum number of results to return. Max 1000.

        Returns:
            list[dict[str, Any]]: The search results.

        Raises:
            ValueError: If the view is not an edge view and nested properties are used, e.g. {"property": ["nested"]}.
            CogniteAPIError: If the view is not found.

        """
        filter = self._equals_none_to_not_exists(filter)
        return self._execute_search(view, properties, query, filter, search_properties, sort, limit)

    def _execute_search(
        self,
        view_id: dm.ViewId,
        properties: SelectedProperties | None = None,
        query: str | None = None,
        filter: filters.Filter | None = None,
        search_properties: str | SequenceNotStr[str] | None = None,
        sort: Sequence[dm.InstanceSort] | dm.InstanceSort | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        view = self._get_view(view_id)
        flatten_props, are_flat_properties = self._as_property_list(properties, "list") if properties else (None, False)
        if properties is None or are_flat_properties:
            instance_types = self._get_instance_types(view)
            all_results: list[dict[str, Any]] = []
            for instance_type in instance_types:
                # The .search has 4 overloads methods and MyPy seems to just give up:
                # 'Not all union combinations were tried because there are too many union'
                # Thus adding an ignore here.
                search_instance_result = self._client.data_modeling.instances.search(  # type: ignore[misc]
                    view_id,
                    query,
                    instance_type=instance_type,  # type: ignore[arg-type]
                    properties=search_properties,  # type: ignore[arg-type]
                    filter=filter,
                    limit=limit or SEARCH_LIMIT,
                    sort=sort,
                )
                all_results.extend(
                    self._prepare_list_result(search_instance_result, set(flatten_props) if flatten_props else None)
                )
            return all_results
        elif view.used_for == "edge":
            raise ValueError("Nested properties are not supported for edges")
        search_result = self._client.data_modeling.instances.search(  # type: ignore[call-overload]
            view_id,
            query,
            instance_type="node",
            properties=search_properties,  # type: ignore[arg-type]
            filter=filter,
            limit=limit or SEARCH_LIMIT,
            sort=sort,
        )
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
                batch_result = self.list(view_id, properties, batch_filter, sort, limit or SEARCH_LIMIT)
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
        """Aggregate nodes/edges in a view.

        Note if the view supports both nodes and edges the result will be a list of nodes and edges.

        Nested properties are not supported for edges.

        Args:
            view: The view in which the nodes/edges have properties.
            aggregates: The aggregations to perform.
            group_by: The properties to group by.
            filter: The filter to apply ahead of the aggregation.
            query: The search query. This is useful when you want to show the number of results
                of a specific search query. It is useful for combining with the search method.
            search_properties: The properties to search. If None, all text properties are searched.
            limit: The maximum number of results to return. Max 1000.

        Returns:
            dict[str, Any] | list[dict[str, Any]]: The aggregation results.

        Raises:
            ValueError: If the view is not an edge view and nested properties are used, e.g. {"property": ["nested"]}.

        """
        filter = self._equals_none_to_not_exists(filter)
        return self._execute_aggregation(view, aggregates, search_properties, query, filter, group_by, limit)

    @classmethod
    def _equals_none_to_not_exists(cls, filter: filters.Filter | None) -> filters.Filter | None:
        """Converts all Equals([property], None) filters to Not(Exists([property])) filters.

        The motivation is that the DMS API does not support Equals([property], None) filters, and
        it is more intuitive to use Equals([property], None) filters in the query builder.
        """
        if isinstance(filter, filters.Equals) and filter._value is None:
            return filters.Not(filters.Exists(filter._property))
        elif isinstance(filter, filters.And):
            return filters.And(*[res for f in filter._filters if (res := cls._equals_none_to_not_exists(f))])
        elif isinstance(filter, filters.Or):
            return filters.Or(*[res for f in filter._filters if (res := cls._equals_none_to_not_exists(f))])
        elif isinstance(filter, filters.Not) and filter._filters:
            if res := cls._equals_none_to_not_exists(filter._filters[0]):
                return filters.Not(res)
        return filter

    def _get_view(self, view_id: dm.ViewId) -> dm.View:
        if view_id not in self._view_by_id:
            view = self._client.data_modeling.views.retrieve(view_id, all_versions=False)
            if not view:
                raise CogniteAPIError(f"View not found: {view_id!r}", code=200)
            self._view_by_id[view_id] = view[0]
        return self._view_by_id[view_id]

    @staticmethod
    def _as_property_list(properties: SelectedProperties, operation: str) -> tuple[list[str], bool]:
        output: list[str] = []
        is_nested_supported = operation == "list"
        are_flat_properties = True
        for prop in properties:
            if isinstance(prop, str):
                output.append(prop)
            elif isinstance(prop, dict) and is_nested_supported:
                if len(prop) != 1:
                    raise ValueError(f"Unexpected nested property: {prop}")
                key = next(iter(prop.keys()))
                output.append(key)
                are_flat_properties = False
            elif isinstance(prop, dict):
                raise ValueError(f"Nested properties are not supported for operation {operation}")
            else:
                raise ValueError(f"Unexpected property type: {type(prop)}")
        return output, are_flat_properties

    def _execute_list(
        self,
        view_id: dm.ViewId,
        properties: SelectedProperties,
        filter: filters.Filter | None = None,
        sort: Sequence[dm.InstanceSort] | dm.InstanceSort | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        view = self._get_view(view_id)
        root_properties, _ = self._as_property_list(properties, "list")
        builder = QueryBuilder()
        factory = QueryBuildStepFactory(
            builder.create_name, view=view, user_selected_properties=properties, unpack_edges=self._unpack_edges
        )

        if not factory.connection_properties:
            list_results: list[dict[str, Any]] = []
            instance_types = self._get_instance_types(view)
            for instance_type in instance_types:
                response = self._client.data_modeling.instances.list(
                    instance_type=instance_type,
                    sources=[view_id],
                    filter=filter,
                    limit=limit,
                    sort=sort,
                )
                list_results.extend(self._prepare_list_result(response, set(root_properties)))
            return list_results

        if view.used_for == "edge":
            raise ValueError("Nested properties are not supported for edges")

        reverse_views = {
            prop.through.source: self._get_view(prop.through.source)
            for prop in factory.reverse_properties.values()
            if isinstance(prop.through.source, dm.ViewId)
        }
        builder.append(factory.root(filter, limit=limit))
        for connection_id, connection in factory.connection_properties.items():
            builder.extend(factory.from_connection(connection_id, connection, reverse_views))
        executor = builder.build()
        results = executor.execute_query(self._client, remove_not_connected=False)
        return QueryUnpacker(
            results, edges=self._unpack_edges, as_data_record=False, edge_type_key="type", node_type_key="type"
        ).unpack()

    @staticmethod
    def _get_instance_types(view: dm.View) -> list[Literal["node", "edge"]]:
        return cast(list[Literal["node", "edge"]], (["node", "edge"] if view.used_for == "all" else [view.used_for]))

    @classmethod
    def _prepare_list_result(
        cls, result: dm.NodeList[dm.Node] | dm.EdgeList[dm.Edge], selected_properties: set[str] | None
    ) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        for instance in result:
            item = QueryUnpacker.flatten_dump(instance, selected_properties)
            if item:
                # As long as you have selected properties, you will not get None.
                output.append(item)  # type: ignore[arg-type]
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

        view = self._get_view(view_id)
        instance_types = self._get_instance_types(view)
        if metric_aggregates and group_by is not None:
            if len(instance_types) == 2 and any(isinstance(agg, Avg) for agg in metric_aggregates):
                # This can be supported (need to add the count of the values to the AvgValue, but will postpone
                # this until we have a use case for it.
                raise ValueError("Average aggregation is not supported for views that is used for both nodes and edges")
            group_results: list[InstanceAggregationResultList] = []
            for instance_type in instance_types:
                group_results.append(
                    self._client.data_modeling.instances.aggregate(  # type: ignore[call-overload]
                        instance_type=instance_type,
                        view=view_id,
                        group_by=group_by,
                        aggregates=metric_aggregates,
                        query=query,
                        properties=search_properties,
                        filter=filter,
                        limit=limit or AGGREGATION_LIMIT,
                    )
                )
            return self._merge_groupby_aggregate_results(group_results)
        elif metric_aggregates:
            aggregate_results: list[list[dm.aggregations.AggregatedNumberedValue]] = []
            for instance_type in instance_types:
                aggregate_results.append(
                    self._client.data_modeling.instances.aggregate(
                        view_id,
                        instance_type=instance_type,
                        aggregates=metric_aggregates,
                        query=query,
                        properties=search_properties,
                        filter=filter,
                        limit=limit or AGGREGATION_LIMIT,
                    )
                )
            return self._merge_aggregate_results(aggregate_results)
        elif histogram_aggregates:
            if len(instance_types) == 2:
                raise ValueError(
                    "Histogram aggregation is not supported for views that is used for both nodes and edges"
                )
            histogram_results = self._client.data_modeling.instances.histogram(
                view_id,
                histograms=histogram_aggregates,
                instance_type=instance_types[0],
                query=query,
                properties=search_properties,  # type: ignore[arg-type]
                filter=filter,
                limit=limit or AGGREGATION_LIMIT,
            )
            return self._histogram_aggregation_to_dict(histogram_results)
        else:
            raise ValueError("No aggregation found")

    @staticmethod
    def _merge_aggregate_results(
        instance_type_results: list[list[dm.aggregations.AggregatedNumberedValue]],
    ) -> dict[str, dict[str, float | int | None]]:
        """Merge the results from all instance types into a single result."""
        if len(instance_type_results) > 2:
            raise ValueError("Cannot merge more than two instance types")

        merged_results: dict[str, dict[str, float | int | None]] = {}
        for instance_type_result in instance_type_results:
            for item in instance_type_result:
                if item._aggregate not in merged_results:
                    merged_results[item._aggregate] = {}
                existing_value = merged_results[item._aggregate].get(item.property, None)
                if existing_value is None:
                    merged_results[item._aggregate][item.property] = item.value
                elif item.value is not None:
                    if isinstance(item, dm.aggregations.SumValue | dm.aggregations.CountValue):
                        new_value = existing_value + item.value
                    elif isinstance(item, dm.aggregations.MaxValue):
                        new_value = max(existing_value, item.value)
                    elif isinstance(item, dm.aggregations.MinValue):
                        new_value = min(existing_value, item.value)
                    elif isinstance(item, dm.aggregations.AvgValue):
                        # We cannot merge AvgValue results without having the count of the values.
                        raise ValueError("AvgValue is not supported for merging")
                    else:
                        raise ValueError(f"Unknown aggregation type: {type(item)}")
                    merged_results[item._aggregate][item.property] = new_value
        return merged_results

    @classmethod
    def _merge_groupby_aggregate_results(
        cls, group_results: list[InstanceAggregationResultList]
    ) -> list[dict[str, Any]]:
        """Merge the results from all instance types into a single result."""
        aggregate_results_by_group: dict[str, list[list[dm.aggregations.AggregatedNumberedValue]]] = defaultdict(list)
        # The group is a dict and thus not hashable, so we need to use json.dumps to create a unique key.
        # and then look it up after merging the results from both instance types (node and edge).
        group_by_key: dict[str, dict[str, str | int | float | bool]] = {}
        for group_result in group_results:
            for item in group_result:
                key = json.dumps(item.group, sort_keys=True)
                aggregate_results_by_group[key].append(item.aggregates)
                group_by_key[key] = item.group
        results: list[dict[str, Any]] = []
        merged_aggregates: dict[str, Any]
        for key, aggregates in aggregate_results_by_group.items():
            merged_aggregates = cls._merge_aggregate_results(aggregates)
            # Mypy does not understand the merged_aggregates is a dict[str, Any] and
            # not a dict[str, str | int | float | bool]
            merged_aggregates["group"] = group_by_key[key]  # type: ignore[assignment]
            results.append(merged_aggregates)
        return results

    @staticmethod
    def _histogram_aggregation_to_dict(aggregation: list[dm.aggregations.HistogramValue]) -> dict[str, Any]:
        output: dict[str, dict[str, Any]] = defaultdict(dict)
        for item in aggregation:
            output[item._aggregate][item.property] = {
                "interval": item.interval,
                "buckets": [bucket.dump() for bucket in item.buckets],
            }
        return dict(output)

    def list(
        self,
        view: dm.ViewId,
        properties: SelectedProperties,
        filter: filters.Filter | None = None,
        sort: Sequence[dm.InstanceSort] | dm.InstanceSort | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """List nodes/edges in a view.

        Note if the view supports both nodes and edges the result will be a list of nodes and edges.

        Nested properties are not supported for edges.

        Args:
            view: The view in which the nodes/edges have properties.
            properties: The properties to include in the result.
            filter: The filter to apply ahead of the list operation.
            sort: The sort order of the results.
            limit: The maximum number of results to return. Pagination is handled automatically.

        Returns:
            list[dict[str, Any]]: The list of nodes/edges in the view.

        Raises:
            ValueError: If the view is not an edge view and nested properties are used, e.g. {"property": ["nested"]}.
            CogniteAPIError: If the view is not found.
        """
        filter = self._equals_none_to_not_exists(filter)
        return self._execute_list(view, properties, filter, sort, limit)
