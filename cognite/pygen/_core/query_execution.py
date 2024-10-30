from typing import Any, Literal

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import filters
from cognite.client.data_classes.data_modeling.views import ReverseDirectRelation, ViewProperty

from .query_builder import QueryBuilder, QueryStep


def execute_query(
    client: CogniteClient,
    view: dm.View,
    operation: Literal["list", "aggregate", "search"],
    properties: list[str],
    filter: filters.Filter | None = None,
    groupby: str | None = None,
) -> dict[str, Any]:
    if operation == "list":
        return _execute_list(client, view, properties, filter)
    else:
        raise NotImplementedError(f"Operation {operation} is not supported")


def _execute_list(
    client: CogniteClient, view: dm.View, properties: list[str], filter: filters.Filter | None = None
) -> dict[str, Any]:
    view_id = view.as_id()
    connection_properties = _get_connection_properties(view.properties, properties)
    if not connection_properties:
        result = client.data_modeling.instances.list(
            instance_type="node",
            sources=[view_id],
            filter=filter,
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
            max_retrieve_limit=-1,
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
                    root_name,
                    dm.query.NodeResultSetExpression(
                        direction="outwards",
                        through=view_id.as_property_ref(connection_id),
                    ),
                    view_id=connection.source,
                )
            )
        elif isinstance(connection, ReverseDirectRelation):
            raise NotImplementedError("Need to lookup reverse property to know whether it is listable")
    raise NotImplementedError("Need to implement query execution")


def _get_connection_properties(
    view_properties: dict[str, ViewProperty], properties: list[str]
) -> dict[str, ViewProperty]:
    output: dict[str, ViewProperty] = {}
    for prop in properties:
        definition = view_properties.get(prop)
        if not definition:
            continue
        if _is_connection(definition):
            output[prop] = definition
    return output


def _is_connection(definition: ViewProperty) -> bool:
    return isinstance(definition, dm.ConnectionDefinition) or (
        isinstance(definition, dm.MappedProperty) and isinstance(definition.type, dm.DirectRelation)
    )
