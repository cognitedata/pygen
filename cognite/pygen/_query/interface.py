import itertools
from collections import defaultdict
from collections.abc import Sequence
from typing import Any, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import filters
from cognite.client.data_classes.aggregations import Aggregation
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils.useful_types import SequenceNotStr

from cognite.pygen._version import __version__

from .builder import QueryBuilder, chunker
from .constants import AGGREGATION_LIMIT, IN_FILTER_CHUNK_SIZE, SEARCH_LIMIT, SelectedProperties
from .processing import QueryUnpacker
from .step import QueryStepFactory


class QueryExecutor:
    """Class for executing queries against the Domain Model Storage (DMS) endpoints in CDF.

    Args:
        client (CogniteClient): An instance of the CogniteClient.
        views (Sequence[dm.View], optional): A list of views to use for the queries. Defaults to None.
            If not passed, the views will be fetched from the server when needed.
    """

    def __init__(self, client: CogniteClient, views: Sequence[dm.View] | None = None):
        self._client = client
        # Used for aggregated logging of requests
        client.config.client_name = f"CognitePygen:{__version__}:QueryExecutor:{client.config.client_name}"
        self._view_by_id: dict[dm.ViewId, dm.View] = {view.as_id(): view for view in views or []}
        self._unpack_edges: Literal["skip", "include"] = "include"

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

        """
        filter = self._equals_none_to_not_exists(filter)
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
        """Aggregate nodes/edges in a view.

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
    def _as_property_list(properties: SelectedProperties, operation: str) -> list[str]:
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

    def _execute_list(
        self,
        view_id: dm.ViewId,
        properties: SelectedProperties,
        filter: filters.Filter | None = None,
        sort: Sequence[dm.InstanceSort] | dm.InstanceSort | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        view = self._get_view(view_id)
        root_properties = self._as_property_list(properties, "list")
        builder = QueryBuilder()
        factory = QueryStepFactory(builder.create_name, view=view, user_selected_properties=properties)

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
        builder.append(factory.root(filter, limit=limit))
        for connection_id, connection in factory.connection_properties.items():
            builder.extend(factory.from_connection(connection_id, connection, reverse_views))
        _ = builder.execute_query(self._client, remove_not_connected=False)
        return QueryUnpacker(
            builder, edges=self._unpack_edges, as_data_record=False, edge_type_key="type", node_type_key="type"
        ).unpack()

    @classmethod
    def _prepare_list_result(
        cls, result: dm.NodeList[dm.Node], selected_properties: set[str] | None
    ) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        for node in result:
            item = QueryUnpacker.flatten_dump(node, selected_properties)
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

    def list(
        self,
        view: dm.ViewId,
        properties: SelectedProperties,
        filter: filters.Filter | None = None,
        sort: Sequence[dm.InstanceSort] | dm.InstanceSort | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """List nodes/edges in a view.

        Args:
            view: The view in which the nodes/edges have properties.
            properties: The properties to include in the result.
            filter: The filter to apply ahead of the list operation.
            sort: The sort order of the results.
            limit: The maximum number of results to return. Pagination is handled automatically.
        """
        filter = self._equals_none_to_not_exists(filter)
        return self._execute_list(view, properties, filter, sort, limit)
