import math
import warnings
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from functools import cached_property
from typing import Any, Literal

from cognite.client import CogniteClient
from cognite.client.data_classes import data_modeling as dm
from cognite.client.data_classes._base import CogniteObject
from cognite.client.data_classes.aggregations import Count
from cognite.client.data_classes.data_modeling.instances import Instance
from cognite.client.data_classes.data_modeling.views import ReverseDirectRelation, ViewProperty
from cognite.client.exceptions import CogniteAPIError

from wind_turbine.data_classes._core.query.constants import (
    ACTUAL_INSTANCE_QUERY_LIMIT,
    INSTANCE_QUERY_LIMIT,
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


class QueryStep:
    def __init__(
        self,
        name: str,
        expression: dm.query.ResultSetExpression,
        view_id: dm.ViewId | None = None,
        max_retrieve_limit: int = -1,
        select: dm.query.Select | None | type[NotSetSentinel] = NotSetSentinel,
        raw_filter: dm.Filter | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        view_property: ViewPropertyId | None = None,
        selected_properties: list[str] | None = None,
    ):
        self.name = name
        self.expression = expression
        self.view_id = view_id
        self.max_retrieve_limit = max_retrieve_limit
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
        self.view_property = view_property
        self.selected_properties = selected_properties
        self._max_retrieve_batch_limit = ACTUAL_INSTANCE_QUERY_LIMIT
        self.cursor: str | None = None
        self.total_retrieved: int = 0
        self.last_batch_count: int = 0
        self.results: list[Instance] = []

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
    def node_results(self) -> Iterable[dm.Node]:
        return (item for item in self.results if isinstance(item, dm.Node))

    @property
    def edge_results(self) -> Iterable[dm.Edge]:
        return (item for item in self.results if isinstance(item, dm.Edge))

    def update_expression_limit(self) -> None:
        if self.is_unlimited:
            self.expression.limit = self._max_retrieve_batch_limit
        else:
            self.expression.limit = max(min(INSTANCE_QUERY_LIMIT, self.max_retrieve_limit - self.total_retrieved), 0)

    def reduce_max_batch_limit(self) -> bool:
        self._max_retrieve_batch_limit = max(1, self._max_retrieve_batch_limit // 2)
        return self._max_retrieve_batch_limit > 1

    @property
    def is_unlimited(self) -> bool:
        return self.max_retrieve_limit in {None, -1, math.inf}

    @property
    def is_finished(self) -> bool:
        return (
            (not self.is_unlimited and self.total_retrieved >= self.max_retrieve_limit)
            or self.cursor is None
            or self.last_batch_count == 0
        )

    def count_total(self, cognite_client: CogniteClient) -> float | None:
        if self.view_id is None:
            # Cannot count the total without a view
            return None
        try:
            return cognite_client.data_modeling.instances.aggregate(
                self.view_id, Count("externalId"), filter=self.raw_filter
            ).value
        except CogniteAPIError:
            return None

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r}, from={self.from_!r}, results={len(self.results)})"


class QueryStepFactory:
    def __init__(
        self,
        view: dm.View,
        user_selected_properties: SelectedProperties,
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
        skip = NODE_PROPERTIES | set(self.reverse_properties.keys())
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
