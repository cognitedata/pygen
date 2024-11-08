import datetime
import math
import time
import warnings
from collections import defaultdict
from collections.abc import Collection, Iterable, Iterator, MutableSequence, Sequence
from contextlib import suppress
from dataclasses import dataclass
from typing import (
    Any,
    Literal,
    SupportsIndex,
    cast,
    overload,
)

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes._base import CogniteObject
from cognite.client.data_classes.aggregations import Count
from cognite.client.data_classes.data_modeling.instances import Instance
from cognite.client.exceptions import CogniteAPIError

DEFAULT_QUERY_LIMIT = 5
INSTANCE_QUERY_LIMIT = 1_000
# The limit used for the In filter in /search
IN_FILTER_CHUNK_SIZE = 100
# This is the actual limit of the API, we typically set it to a lower value to avoid hitting the limit.
# The actual instance query limit is 10_000, but we set it to 5_000 such that is matches the In filter
# which we use in /search for reverse of list direct relations.
ACTUAL_INSTANCE_QUERY_LIMIT = 5_000
# The minimum estimated seconds before print progress on a query
MINIMUM_ESTIMATED_SECONDS_BEFORE_PRINT_PROGRESS = 30
PRINT_PROGRESS_PER_N_NODES = 10_000
SEARCH_LIMIT = 1_000
AGGREGATION_LIMIT = 1_000


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


class _NotSetSentinel:
    """This is a special class that indicates that a value has not been set.
    It is used when we need to distinguish between not set and None."""

    ...


class QueryReducingBatchSize(UserWarning):
    """Raised when a query is too large and the batch size must be reduced."""

    ...


def chunker(sequence: Sequence, chunk_size: int) -> Iterator[Sequence]:
    """
    Split a sequence into chunks of size chunk_size.

    Args:
        sequence: The sequence to split.
        chunk_size: The size of each chunk.

    Returns:
        An iterator over the chunks.

    """
    for i in range(0, len(sequence), chunk_size):
        yield sequence[i : i + chunk_size]


class QueryStep:
    def __init__(
        self,
        name: str,
        expression: dm.query.ResultSetExpression,
        view_id: dm.ViewId | None = None,
        max_retrieve_limit: int = -1,
        select: dm.query.Select | None | type[_NotSetSentinel] = _NotSetSentinel,
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
        if select is _NotSetSentinel:
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
            # Single direct relations are dependent on the parent node,
            # so we assume that the parent node is the limiting factor.
            or self.is_single_direct_relation
        )

    def count_total(self, cognite_client: CogniteClient) -> float:
        if self.view_id is None:
            raise ValueError("Cannot count total if select is not set")

        return cognite_client.data_modeling.instances.aggregate(
            self.view_id, Count("externalId"), filter=self.raw_filter
        ).value

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r}, from={self.from_!r}, results={len(self.results)})"


class QueryBuilder(list, MutableSequence[QueryStep]):
    """This is a helper class to build and execute a query. It is responsible for
    doing the paging of the query and keeping track of the results."""

    def __init__(self, steps: Collection[QueryStep] | None = None):
        super().__init__(steps or [])

    def _reset(self):
        for expression in self:
            expression.total_retrieved = 0
            expression.cursor = None
            expression.results = []

    def _update_expression_limits(self) -> None:
        for expression in self:
            expression.update_expression_limit()

    def _build(self) -> tuple[dm.query.Query, list[QueryStep], set[str]]:
        with_ = {step.name: step.expression for step in self if step.is_queryable}
        select = {step.name: step.select for step in self if step.select is not None and step.is_queryable}
        cursors = self._cursors

        step_by_name = {step.name: step for step in self}
        search: list[QueryStep] = []
        temporary_select: set[str] = set()
        for step in self:
            if step.is_queryable:
                continue
            if step.node_expression is not None:
                search.append(step)
                # Ensure that select is set for the parent
                if step.from_ in select or step.from_ is None:
                    continue
                view_id = step_by_name[step.from_].view_id
                if view_id is None:
                    continue
                select[step.from_] = dm.query.Select([dm.query.SourceSelector(view_id, ["*"])])
                temporary_select.add(step.from_)
        return dm.query.Query(with_=with_, select=select, cursors=cursors), search, temporary_select

    def _dump_yaml(self) -> str:
        return self._build()[0].dump_yaml()

    @property
    def _cursors(self) -> dict[str, str | None]:
        return {expression.name: expression.cursor for expression in self if expression.is_queryable}

    def _update(self, batch: dm.query.QueryResult):
        for expression in self:
            if expression.name not in batch:
                continue
            expression.last_batch_count = len(batch[expression.name])
            expression.total_retrieved += expression.last_batch_count
            expression.cursor = batch.cursors.get(expression.name)
            expression.results.extend(batch[expression.name].data)

    @property
    def _is_finished(self) -> bool:
        return self[0].is_finished

    def _reduce_max_batch_limit(self) -> bool:
        for expression in self:
            if not expression.reduce_max_batch_limit():
                return False
        return True

    def execute_query(self, client: CogniteClient, remove_not_connected: bool = False) -> dict[str, list[Instance]]:
        self._reset()
        query, to_search, temp_select = self._build()

        if not self:
            raise ValueError("No query steps to execute")

        count: float | None = None
        with suppress(ValueError, CogniteAPIError):
            count = self[0].count_total(client)

        is_large_query = False
        last_progress_print = 0
        nodes_per_second = 0.0
        while True:
            self._update_expression_limits()
            query.cursors = self._cursors
            t0 = time.time()
            try:
                batch = client.data_modeling.instances.query(query)
            except CogniteAPIError as e:
                if e.code == 408:
                    # Too big query, try to reduce the limit
                    if self._reduce_max_batch_limit():
                        continue
                    new_limit = self[0]._max_retrieve_batch_limit
                    warnings.warn(
                        f"Query is too large, reducing batch size to {new_limit:,}, and trying again",
                        QueryReducingBatchSize,
                        stacklevel=2,
                    )

                raise e

            self._fetch_reverse_direct_relation_of_lists(client, to_search, batch)

            for name in temp_select:
                batch.pop(name, None)

            last_execution_time = time.time() - t0

            self._update(batch)
            if self._is_finished:
                break

            if count is None:
                continue
            # Estimate the number of nodes per second using exponential moving average
            last_batch_nodes_per_second = len(batch[self[0].name]) / last_execution_time
            if nodes_per_second == 0.0:
                nodes_per_second = last_batch_nodes_per_second
            else:
                nodes_per_second = 0.1 * last_batch_nodes_per_second + 0.9 * nodes_per_second
            # Estimate the time to completion
            remaining_nodes = count - self[0].total_retrieved
            remaining_time = remaining_nodes / nodes_per_second

            if is_large_query and (self[0].total_retrieved - last_progress_print) > PRINT_PROGRESS_PER_N_NODES:
                estimate = datetime.timedelta(seconds=round(remaining_time, 0))
                print(
                    f"Progress: {self[0].total_retrieved:,}/{count:,} nodes retrieved. "
                    f"Estimated time to completion: {estimate}"
                )
                last_progress_print = self[0].total_retrieved

            if is_large_query is False and remaining_time > MINIMUM_ESTIMATED_SECONDS_BEFORE_PRINT_PROGRESS:
                is_large_query = True
                print("Large query detected. Will print progress.")

        if remove_not_connected and len(self) > 1:
            _QueryResultCleaner(self).clean()

        return {step.name: step.results for step in self}

    @staticmethod
    def _fetch_reverse_direct_relation_of_lists(
        client: CogniteClient, to_search: list[QueryStep], batch: dm.query.QueryResult
    ) -> None:
        """Reverse direct relations for lists are not supported by the query API.
        This method fetches them separately."""
        for step in to_search:
            if step.from_ is None or step.from_ not in batch:
                continue
            item_ids = [node.as_id() for node in batch[step.from_].data]
            if not item_ids:
                continue

            view_id = step.view_id
            expression = step.node_expression
            if view_id is None or expression is None:
                raise ValueError(
                    "Invalid state of the query. Search should always be a node expression with view properties"
                )
            if expression.through is None:
                raise ValueError("Missing through set in a reverse-list query")
            limit = SEARCH_LIMIT if step.is_unlimited else min(step.max_retrieve_limit, SEARCH_LIMIT)

            step_result = dm.NodeList[dm.Node]([])
            for item_ids_chunk in chunker(item_ids, IN_FILTER_CHUNK_SIZE):
                is_items = dm.filters.In(view_id.as_property_ref(expression.through.property), item_ids_chunk)
                is_selected = is_items if step.raw_filter is None else dm.filters.And(is_items, step.raw_filter)

                chunk_result = client.data_modeling.instances.search(
                    view_id, properties=None, filter=is_selected, limit=limit
                )
                step_result.extend(chunk_result)

            batch[step.name] = dm.NodeListWithCursor(step_result, None)
        return None

    def get_from(self) -> str | None:
        if len(self) == 0:
            return None
        return self[-1].name

    def create_name(self, from_: str | None) -> str:
        if from_ is None:
            return "0"
        return f"{from_}_{len(self)}"

    def append(self, __object: QueryStep, /) -> None:
        # Extra validation to ensure all assumptions are met
        if len(self) == 0:
            if __object.from_ is not None:
                raise ValueError("The first step should not have a 'from_' value")
        else:
            if __object.from_ is None:
                raise ValueError("The 'from_' value should be set")
        super().append(__object)

    def extend(self, __iterable: Iterable[QueryStep], /) -> None:
        for item in __iterable:
            self.append(item)

    # The implementations below are to get proper type hints
    def __iter__(self) -> Iterator[QueryStep]:
        return super().__iter__()

    @overload
    def __getitem__(self, item: SupportsIndex) -> QueryStep: ...

    @overload
    def __getitem__(self, item: slice) -> "QueryBuilder": ...

    def __getitem__(self, item: SupportsIndex | slice) -> "QueryStep | QueryBuilder":
        value = super().__getitem__(item)
        if isinstance(item, slice):
            return QueryBuilder(value)  # type: ignore[arg-type]
        return cast(QueryStep, value)


class _QueryResultCleaner:
    """Remove nodes and edges that are not connected through the entire query"""

    def __init__(self, steps: list[QueryStep]):
        self._tree = self._create_tree(steps)
        self._root = steps[0]

    @classmethod
    def _create_tree(cls, steps: list[QueryStep]) -> dict[str, list[QueryStep]]:
        tree: dict[str, list[QueryStep]] = defaultdict(list)
        for step in steps:
            if step.from_ is None:
                continue
            tree[step.from_].append(step)
        return dict(tree)

    def clean(self) -> None:
        self._clean(self._root)

    @staticmethod
    def as_node_id(direct_relation: dm.DirectRelationReference | dict[str, str]) -> dm.NodeId:
        if isinstance(direct_relation, dict):
            return dm.NodeId(direct_relation["space"], direct_relation["externalId"])

        return dm.NodeId(direct_relation.space, direct_relation.external_id)

    def _clean(self, step: QueryStep) -> tuple[set[dm.NodeId], str | None]:
        if step.name not in self._tree:
            # Leaf Node
            direct_relation: str | None = None
            if step.node_expression and (through := step.node_expression.through) is not None:
                direct_relation = through.property
                if step.node_expression.direction == "inwards":
                    return {
                        node_id for item in step.node_results for node_id in self._get_relations(item, direct_relation)
                    }, None

            return {item.as_id() for item in step.results}, direct_relation  # type: ignore[attr-defined]

        expected_ids_by_property: dict[str | None, set[dm.NodeId]] = {}
        for child in self._tree[step.name]:
            child_ids, property_id = self._clean(child)
            if property_id not in expected_ids_by_property:
                expected_ids_by_property[property_id] = child_ids
            else:
                expected_ids_by_property[property_id] |= child_ids

        if step.node_expression is not None:
            filtered_results: list[Instance] = []
            for node in step.node_results:
                if self._is_connected_node(node, expected_ids_by_property):
                    filtered_results.append(node)
            step.results = filtered_results
            direct_relation = None if step.node_expression.through is None else step.node_expression.through.property
            return {node.as_id() for node in step.node_results}, direct_relation

        if step.edge_expression:
            if len(expected_ids_by_property) > 1 or None not in expected_ids_by_property:
                raise RuntimeError(f"Invalid state of {type(self).__name__}")
            expected_ids = expected_ids_by_property[None]
            if step.edge_expression.direction == "outwards":
                step.results = [edge for edge in step.edge_results if self.as_node_id(edge.end_node) in expected_ids]
                return {self.as_node_id(edge.start_node) for edge in step.edge_results}, None
            else:  # inwards
                step.results = [edge for edge in step.edge_results if self.as_node_id(edge.start_node) in expected_ids]
                return {self.as_node_id(edge.end_node) for edge in step.edge_results}, None

        raise TypeError(f"Unsupported query step type: {type(step)}")

    @classmethod
    def _is_connected_node(cls, node: dm.Node, expected_ids_by_property: dict[str | None, set[dm.NodeId]]) -> bool:
        if not expected_ids_by_property:
            return True
        if None in expected_ids_by_property:
            if node.as_id() in expected_ids_by_property[None]:
                return True
            if len(expected_ids_by_property) == 1:
                return False
        node_properties = next(iter(node.properties.values()))
        for property_id, expected_ids in expected_ids_by_property.items():
            if property_id is None:
                continue
            value = node_properties.get(property_id)
            if value is None:
                continue
            elif isinstance(value, list):
                if {cls.as_node_id(item) for item in value if isinstance(item, dict)} & expected_ids:
                    return True
            elif isinstance(value, dict) and cls.as_node_id(value) in expected_ids:
                return True
        return False

    @classmethod
    def _get_relations(cls, node: dm.Node, property_id: str) -> Iterable[dm.NodeId]:
        if property_id is None:
            return {node.as_id()}
        value = next(iter(node.properties.values())).get(property_id)
        if isinstance(value, list):
            return [cls.as_node_id(item) for item in value if isinstance(item, dict)]
        elif isinstance(value, dict):
            return [cls.as_node_id(value)]
        return []
