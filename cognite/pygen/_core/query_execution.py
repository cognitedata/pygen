from collections.abc import Sequence
from typing import Any, Literal

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import filters
from cognite.client.data_classes.aggregations import MetricAggregation
from cognite.client.data_classes.data_modeling.views import ReverseDirectRelation, ViewProperty
from cognite.client.exceptions import CogniteAPIError

from .query_builder import SEARCH_LIMIT, QueryBuilder, QueryStep


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

    def execute_query(
        self,
        view: dm.ViewId,
        operation: Literal["list", "aggregate", "search"],
        properties: list[str],
        filter: filters.Filter | None = None,
        query: str | None = None,
        groupby: str | None = None,
        aggregates: MetricAggregation | Sequence[MetricAggregation] | None = None,
        sort: dm.InstanceSort | Sequence[dm.InstanceSort] | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        if operation == "list":
            return self._execute_list(view, properties, filter, sort, limit)
        elif operation == "aggregate":
            aggregate_result = self._client.data_modeling.instances.aggregate(  # type: ignore[misc]
                view,
                aggregates=aggregates,  # type: ignore[arg-type]
                group_by=groupby,  # type: ignore[arg-type]
                query=query,
                properties=properties,
                filter=filter,
                limit=limit,  # type: ignore[arg-type]
            )
            return {"items": aggregate_result.dump()}
        elif operation == "search":
            search_result = self._client.data_modeling.instances.search(
                view, query, properties=properties, filter=filter, limit=limit or SEARCH_LIMIT, sort=sort
            )
            return {"items": search_result.dump()}
        else:
            raise NotImplementedError(f"Operation {operation} is not supported")

    def _execute_list(
        self,
        view_id: dm.ViewId,
        properties: list[str],
        filter: filters.Filter | None = None,
        sort: Sequence[dm.InstanceSort] | dm.InstanceSort | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        view = self._get_view(view_id)
        connection_properties = self._get_connection_properties(view.properties, properties)

        if not connection_properties:
            result = self._client.data_modeling.instances.list(
                instance_type="node",
                sources=[view_id],
                filter=filter,
                limit=limit,
                sort=sort,
            )
            return {"items": result.dump()}

        builder = QueryBuilder()
        has_data = dm.filters.HasData(views=[view_id])
        root_name = builder.create_name(None)
        builder.append(
            QueryStep(
                root_name,
                dm.query.NodeResultSetExpression(
                    filter=dm.filters.And(filter, has_data) if filter else has_data,
                ),
                view_id=view.as_id(),
                max_retrieve_limit=-1 if limit is None else limit,
                raw_filter=filter,
            )
        )
        for connection_id, connection in connection_properties.items():
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
                        view_id=target_view,
                    )
                )
            elif isinstance(connection, dm.MappedProperty) and isinstance(connection.type, dm.DirectRelation):
                builder.append(
                    QueryStep(
                        builder.create_name(root_name),
                        dm.query.NodeResultSetExpression(
                            from_=root_name,
                            direction="outwards",
                            through=view_id.as_property_ref(connection_id),
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
                        view_id=connection.source,
                        connection_type=connection_type,
                    )
                )
        return builder.execute_query(self._client, remove_not_connected=False)

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
