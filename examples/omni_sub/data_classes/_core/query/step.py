import math
import warnings
from collections.abc import Callable, Iterable, MutableSequence
from dataclasses import dataclass
from functools import cached_property
from typing import Any, Literal

from cognite.client import CogniteClient
from cognite.client.data_classes import data_modeling as dm
from cognite.client.data_classes._base import CogniteObject
from cognite.client.data_classes.data_modeling.instances import Instance
from cognite.client.data_classes.data_modeling.views import ReverseDirectRelation, ViewProperty

from omni_sub.data_classes._core.query.constants import (
    ACTUAL_INSTANCE_QUERY_LIMIT,
    NODE_PROPERTIES,
    NotSetSentinel,
    SelectedProperties,
)


@dataclass(frozen=True)
class ViewPropertyId(CogniteObject):
    view: dm.ViewId
    property: str

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> "ViewPropertyId":
        return cls(
            view=dm.ViewId.load(resource["view"]),
            property=resource["identifier"],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "view": self.view.dump(camel_case=camel_case, include_type=False),
            "identifier": self.property,
        }


class QueryBuildStep:
    """QueryStep represents a single step in a query execution.

    It is used to keep track of the state of a query step, such as the current cursor, the total number of retrieved
    instances, and the results of the query.

    Args:
        name: The unique name of the step.
        expression: The node or edge expression passed to the API.
        view_id: The view ID representing the view to query.
        max_retrieve_limit: The maximum number of instances to retrieve. Defaults to -1. If set to -1, it will
            continue to retrieve instances until there are no more instances to retrieve.
        max_retrieve_batch_limit: The maximum number of instances to retrieve in a single batch. Defaults to -1.
        select: The selected properties to retrieve. If not set, it will default to all properties. None indicates
            to not retrieve any properties from the view.
        raw_filter: This is the same filter as the expression, but without the HasData filter. This is used to count
            the total number of instances in the view such that the ETA can be calculated.
        connection_type: The connection type. Defaults to None.
            It is used to deal with the special case of reverse-list connections, which requires using the search
            endpoint instead of the query endpoint.
        connection_property: The property ID of the connection from the view referenced in the expression.from_
            that is used to connect to the view in this step. Defaults to None, which indicates that this is the
            root step.
        selected_properties: The user selected properties to retrieve. Defaults to None, which indicates that all
            properties should be retrieved.

    """

    def __init__(
        self,
        name: str,
        expression: dm.query.NodeOrEdgeResultSetExpression,
        view_id: dm.ViewId | None = None,
        max_retrieve_limit: int = -1,
        max_retrieve_batch_limit: int | None = None,
        select: dm.query.Select | None | type[NotSetSentinel] = NotSetSentinel,
        raw_filter: dm.Filter | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        connection_property: ViewPropertyId | None = None,
        selected_properties: list[str] | None = None,
    ):
        self.name = name
        self.expression = expression
        self.view_id = view_id
        self.max_retrieve_limit = max_retrieve_limit
        self._max_retrieve_batch_limit = max_retrieve_batch_limit
        self.select: dm.query.Select | None
        if select is NotSetSentinel:
            try:
                self.select = self._default_select()
            except NotImplementedError:
                raise ValueError(f"You need to provide a select to instantiate a {type(self).__name__}") from None
        else:
            self.select = select  # type: ignore[assignment]
        self.raw_filter = raw_filter
        self.connection_type = connection_type
        self.connection_property = connection_property
        self.selected_properties = selected_properties

    def _default_select(self) -> dm.query.Select:
        if self.view_id is None:
            return dm.query.Select()
        else:
            return dm.query.Select([dm.query.SourceSelector(self.view_id, ["*"])])

    @property
    def is_queryable(self) -> bool:
        # We cannot query across reverse-list connections
        return self.connection_type != "reverse-list"

    @property
    def from_(self) -> str | None:
        return self.expression.from_

    @property
    def is_single_direct_relation(self) -> bool:
        return isinstance(self.expression, dm.query.NodeResultSetExpression) and self.expression.through is not None

    @property
    def node_expression(self) -> dm.query.NodeResultSetExpression | None:
        if isinstance(self.expression, dm.query.NodeResultSetExpression):
            return self.expression
        return None

    @property
    def edge_expression(self) -> dm.query.EdgeResultSetExpression | None:
        if isinstance(self.expression, dm.query.EdgeResultSetExpression):
            return self.expression
        return None

    @property
    def is_unlimited(self) -> bool:
        return self.max_retrieve_limit in {None, -1, math.inf}

    @property
    def max_retrieve_batch_limit(self) -> int:
        if self._max_retrieve_batch_limit is None or self._max_retrieve_batch_limit in {-1, math.inf}:
            return ACTUAL_INSTANCE_QUERY_LIMIT
        return max(1, min(self._max_retrieve_batch_limit, ACTUAL_INSTANCE_QUERY_LIMIT))

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r}, from={self.from_!r})"


class QueryBuildStepFactory:
    """QueryStepFactory is a factory class that creates query steps.

    Args:
        create_step_name: A function that creates a unique name for a step.
        view_id: The view ID to query. Either view_id or view must be set.
        view: The view to query. Either view_id or view must be set.
        user_selected_properties: The user selected properties to retrieve. Defaults to None, which indicates that all
            properties should be retrieved.

    """

    def __init__(
        self,
        create_step_name: Callable[[str | None], str],
        view_id: dm.ViewId | None = None,
        view: dm.View | None = None,
        user_selected_properties: SelectedProperties | None = None,
        edge_connection_property: str = "node",
        unpack_edges: Literal["skip", "include"] = "include",
    ) -> None:
        if sum(1 for x in (view_id, view) if x is not None) != 1:
            raise ValueError("Either view_id or view must be set")
        self._create_step_name = create_step_name
        if view_id is not None:
            self._view_id = view_id
        elif view is not None:
            self._view_id = view.as_id()
        else:
            raise ValueError("Either view_id or view must be set")
        self._view = view
        self._user_selected_properties = user_selected_properties
        self._root_name: str | None = None
        self._edge_connection_property = edge_connection_property
        self._unpack_edges = unpack_edges

    @property
    def root_name(self) -> str:
        if self._root_name is None:
            raise ValueError("Root step is not created")
        return self._root_name

    @cached_property
    def _root_properties(self) -> list[str] | None:
        if self._user_selected_properties is None:
            return None
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
        if self._root_properties is None or self._view is None:
            raise ValueError("View or user selected properties is required for finding" " connection properties")
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
    def _nested_properties_by_property(self) -> dict[str, list[str | dict[str, list[str]]]] | None:
        if self._user_selected_properties is None:
            return None
        nested_properties_by_property: dict[str, list[str | dict[str, list[str]]]] = {}
        for prop in self._user_selected_properties:
            if isinstance(prop, dict):
                key, value = next(iter(prop.items()))
                nested_properties_by_property[key] = value
        return nested_properties_by_property

    def root(
        self,
        filter: dm.Filter | None = None,
        sort: list[dm.InstanceSort] | None = None,
        limit: int | None = None,
        has_container_fields: bool = True,
        max_retrieve_batch_limit: int | None = None,
    ) -> QueryBuildStep:
        if self._root_properties:
            skip = NODE_PROPERTIES | set(self.reverse_properties.keys())
            select = self._create_select([prop for prop in self._root_properties if prop not in skip], self._view_id)
            selected_properties = self._root_properties
        else:
            select = dm.query.Select([dm.query.SourceSelector(self._view_id, ["*"])])
            selected_properties = None

        if self._root_name is not None:
            raise ValueError("Root step is already created")
        self._root_name = self._create_step_name(None)
        return QueryBuildStep(
            self._root_name,
            dm.query.NodeResultSetExpression(
                filter=self._full_filter(filter, has_container_fields, self._view_id),
                sort=sort,
            ),
            select=select,
            selected_properties=selected_properties,
            view_id=self._view_id,
            max_retrieve_limit=-1 if limit is None else limit,
            raw_filter=filter,
            max_retrieve_batch_limit=max_retrieve_batch_limit,
        )

    def from_connection(
        self, connection_id: str, connection: ViewProperty, reverse_views: dict[dm.ViewId, dm.View]
    ) -> list[QueryBuildStep]:
        connection_property = ViewPropertyId(self._view_id, connection_id)
        selected_properties: list[str | dict[str, list[str]]] = ["*"]
        if (nested := self._nested_properties_by_property) and connection_id in nested:
            selected_properties = nested[connection_id]

        if isinstance(connection, dm.EdgeConnection):
            return self.from_edge(connection.source, connection.direction, connection_property, selected_properties)
        elif isinstance(connection, ReverseDirectRelation):
            connection_type: Literal["reverse-list"] | None = (
                "reverse-list" if self._is_listable(connection.through, reverse_views) else None
            )
            validated = self._validate_flat_properties(selected_properties)
            return self.from_reverse_relation(
                connection.source, connection.through, connection_type, connection_property, validated
            )
        elif isinstance(connection, dm.MappedProperty) and isinstance(connection.type, dm.DirectRelation):
            validated = self._validate_flat_properties(selected_properties)
            return self.from_direct_relation(connection.source, connection_property, validated)
        else:
            warnings.warn(f"Unexpected connection type: {connection!r}", UserWarning, stacklevel=2)
        return []

    def from_direct_relation(
        self,
        source: dm.ViewId | None,
        connection_property: ViewPropertyId,
        selected_properties: list[str] | None = None,
        has_container_fields: bool = True,
    ) -> list[QueryBuildStep]:
        if source is None:
            raise ValueError("Source view not found")
        query_properties = self._create_query_properties(selected_properties, None)
        return [
            QueryBuildStep(
                self._create_step_name(self.root_name),
                dm.query.NodeResultSetExpression(
                    filter=self._full_filter(None, has_container_fields, source),
                    from_=self.root_name,
                    direction="outwards",
                    through=self._view_id.as_property_ref(connection_property.property),
                ),
                connection_property=connection_property,
                select=self._create_select(query_properties, source),
                selected_properties=selected_properties,
                view_id=source,
            )
        ]

    def from_edge(
        self,
        source: dm.ViewId,
        direction: Literal["outwards", "inwards"],
        connection_property: ViewPropertyId,
        selected_properties: list[str | dict[str, list[str]]] | None = None,
        include_end_node: bool = True,
        has_container_fields: bool = True,
        edge_view: dm.ViewId | None = None,
    ) -> list[QueryBuildStep]:
        edge_name = self._create_step_name(self._root_name)
        steps = [
            QueryBuildStep(
                edge_name,
                dm.query.EdgeResultSetExpression(
                    from_=self._root_name,
                    direction=direction,
                    chain_to="source" if direction == "outwards" else "destination",
                ),
                view_id=edge_view,
                selected_properties=[prop for prop in selected_properties or [] if isinstance(prop, str)] or None,
                connection_property=connection_property,
            )
        ]
        if not include_end_node:
            return steps

        if self._unpack_edges == "include":
            node_properties = next(
                (prop for prop in selected_properties or [] if isinstance(prop, dict) and "node" in prop), None
            )
            selected_node_properties: list[str] | None = None
            if isinstance(node_properties, dict) and self._edge_connection_property in node_properties:
                selected_node_properties = node_properties[self._edge_connection_property]
        elif self._unpack_edges == "skip":
            selected_node_properties = [prop for prop in selected_properties or [] if isinstance(prop, str)]
        else:
            raise ValueError(f"Unexpected unpack_edges value: {self._unpack_edges!r}. Expected 'include' or 'skip'.")

        query_properties = self._create_query_properties(selected_node_properties, None)
        target_view = source

        step = QueryBuildStep(
            self._create_step_name(edge_name),
            dm.query.NodeResultSetExpression(
                from_=edge_name,
                filter=self._full_filter(None, has_container_fields, target_view),
            ),
            select=self._create_select(query_properties, target_view),
            selected_properties=selected_node_properties,
            connection_property=ViewPropertyId(target_view, self._edge_connection_property),
            view_id=target_view,
        )
        steps.append(step)
        return steps

    def from_reverse_relation(
        self,
        source: dm.ViewId,
        through: dm.PropertyId,
        connection_type: Literal["reverse-list"] | None,
        connection_property: ViewPropertyId,
        selected_properties: list[str] | None = None,
        has_container_fields: bool = True,
    ) -> list[QueryBuildStep]:
        query_properties = self._create_query_properties(selected_properties, through.property)
        other_view_id = source
        return [
            QueryBuildStep(
                self._create_step_name(self._root_name),
                dm.query.NodeResultSetExpression(
                    from_=self._root_name,
                    direction="inwards",
                    filter=self._full_filter(None, has_container_fields, other_view_id),
                    through=other_view_id.as_property_ref(through.property),
                ),
                connection_property=connection_property,
                select=self._create_select(query_properties, other_view_id),
                selected_properties=selected_properties,
                view_id=source,
                connection_type=connection_type,
            )
        ]

    @classmethod
    def _create_query_properties(
        cls, properties: list[str] | None, connection_id: str | None = None
    ) -> list[str] | None:
        if properties is None:
            return None
        include_connection_prop = "*" not in properties
        nested_properties: list[str] = []
        for prop_id in properties:
            if prop_id in NODE_PROPERTIES:
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
    def _create_select(properties: list[str] | None, view_id: dm.ViewId) -> dm.query.Select:
        properties = properties or ["*"]
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

    @staticmethod
    def _full_filter(filter: dm.Filter | None, has_container_fields: bool, view_id: dm.ViewId) -> dm.Filter | None:
        if has_container_fields:
            has_data = dm.filters.HasData(views=[view_id])
            return dm.filters.And(filter, has_data) if filter else has_data

        return filter


class QueryResultStep(QueryBuildStep):
    def __init__(
        self,
        results: dm.NodeListWithCursor | dm.EdgeListWithCursor,
        name: str,
        expression: dm.query.NodeOrEdgeResultSetExpression,
        view_id: dm.ViewId | None = None,
        max_retrieve_limit: int = -1,
        select: dm.query.Select | None | type[NotSetSentinel] = NotSetSentinel,
        raw_filter: dm.Filter | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        connection_property: ViewPropertyId | None = None,
        selected_properties: list[str] | None = None,
    ) -> None:
        super().__init__(
            name=name,
            expression=expression,
            view_id=view_id,
            max_retrieve_limit=max_retrieve_limit,
            select=select,
            raw_filter=raw_filter,
            connection_type=connection_type,
            connection_property=connection_property,
            selected_properties=selected_properties,
        )
        self.results: list[Instance] = list(results)

    @classmethod
    def from_build(
        cls, results: dm.NodeListWithCursor | dm.EdgeListWithCursor, build: QueryBuildStep
    ) -> "QueryResultStep":
        return cls(
            results=results,
            name=build.name,
            expression=build.expression,
            view_id=build.view_id,
            max_retrieve_limit=build.max_retrieve_limit,
            select=build.select,
            raw_filter=build.raw_filter,
            connection_type=build.connection_type,
            connection_property=build.connection_property,
            selected_properties=build.selected_properties,
        )

    @property
    def node_results(self) -> Iterable[dm.Node]:
        return (item for item in self.results if isinstance(item, dm.Node))

    @property
    def edge_results(self) -> Iterable[dm.Edge]:
        return (item for item in self.results if isinstance(item, dm.Edge))

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r}, from={self.from_!r}, results={len(self.results)})"


class QueryResultStepList(list, MutableSequence[QueryResultStep]):
    """A list of QueryResultStep objects."""

    def __init__(self, *args: QueryResultStep, cursors: dict[str, str | None] | None = None) -> None:
        super().__init__(args)
        self._cursors = cursors or {}

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(steps={len(self)})"
