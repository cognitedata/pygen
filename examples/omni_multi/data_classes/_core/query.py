from __future__ import annotations

import datetime
import math
import time
import warnings
from abc import ABC
from collections import defaultdict
from collections.abc import Collection
from collections.abc import MutableSequence, Iterable
from contextlib import suppress
from typing import (
    cast,
    ClassVar,
    Generic,
    Any,
    Iterator,
    TypeVar,
    overload,
    Union,
    SupportsIndex,
    Literal,
)

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.aggregations import Count
from cognite.client.data_classes.data_modeling.instances import Instance
from cognite.client.exceptions import CogniteAPIError

from .base import (
    DomainModelList,
    T_DomainList,
    DomainRelationList,
    DomainModelCore,
    T_DomainModelList,
    DomainRelation,
    DomainModel,
)
from .constants import (
    _NotSetSentinel,
    DEFAULT_QUERY_LIMIT,
    ACTUAL_INSTANCE_QUERY_LIMIT,
    INSTANCE_QUERY_LIMIT,
    MINIMUM_ESTIMATED_SECONDS_BEFORE_PRINT_PROGRESS,
    PRINT_PROGRESS_PER_N_NODES,
)
from .helpers import as_node_id


T_DomainListEnd = TypeVar("T_DomainListEnd", bound=Union[DomainModelList, DomainRelationList], covariant=True)


class QueryReducingBatchSize(UserWarning):
    """Raised when a query is too large and the batch size must be reduced."""

    ...


class QueryCore(Generic[T_DomainList, T_DomainListEnd]):
    _view_id: ClassVar[dm.ViewId]
    _result_list_cls_end: type[T_DomainListEnd]
    _result_cls: ClassVar[type[DomainModelCore]]

    def __init__(
        self,
        created_types: set[type],
        creation_path: "list[QueryCore]",
        client: CogniteClient,
        result_list_cls: type[T_DomainList],
        expression: dm.query.ResultSetExpression | None = None,
        view_filter: dm.filters.Filter | None = None,
        connection_name: str | None = None,
    ):
        created_types.add(type(self))
        self._creation_path = creation_path[:] + [self]
        self._client = client
        self._result_list_cls = result_list_cls
        self._view_filter = view_filter
        self._expression = expression or dm.query.NodeResultSetExpression()
        self._connection_name = connection_name
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.space = StringFilter(self, ["node", "space"])
        self._filter_classes: list[Filtering] = [self.external_id, self.space]

    @property
    def _connection_names(self) -> set[str]:
        return {step._connection_name for step in self._creation_path if step._connection_name}

    def __getattr__(self, item: str) -> Any:
        if item in self._connection_names:
            nodes = [step._result_cls.__name__ for step in self._creation_path]
            raise ValueError(f"Circular reference detected. Cannot query a circular reference: {nodes}")
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{item}'")

    def _assemble_filter(self) -> dm.filters.Filter:
        filters: list[dm.filters.Filter] = [self._view_filter] if self._view_filter else []
        for filter_cls in self._filter_classes:
            if item := filter_cls._as_filter():
                filters.append(item)
        return dm.filters.And(*filters)

    def _repr_html_(self) -> str:
        nodes = [step._result_cls.__name__ for step in self._creation_path]
        edges = [step._connection_name or "missing" for step in self._creation_path[1:]]
        w = 120
        h = 40
        circles = "    \n".join(f'<circle cx="{i * w + 40}" cy="{h}" r="2" />' for i in range(len(nodes)))
        circle_text = "    \n".join(
            f'<text x="{i * w + 40}" y="{h}" dy="-10">{node}</text>' for i, node in enumerate(nodes)
        )
        arrows = "    \n".join(
            f'<path id="arrow-line"  marker-end="url(#head)" stroke-width="2" fill="none" stroke="black" d="M{i*w+40},{h}, {i*w + 150} {h}" />'
            for i in range(len(edges))
        )
        arrow_text = "    \n".join(
            f'<text x="{i*w+40+120/2}" y="{h}" dy="-5">{edge}</text>' for i, edge in enumerate(edges)
        )

        return f"""<h5>Query</h5>
<div>
<svg height="50" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <marker
      id='head'
      orient="auto"
      markerWidth='3'
      markerHeight='4'
      refX='0.1'
      refY='2'
    >
      <path d='M0,0 V4 L2,2 Z' fill="black" />
    </marker>
  </defs>

    {arrows}

<g stroke="black" stroke-width="3" fill="black">
    {circles}
</g>
<g font-size="10" font-family="sans-serif" text-anchor="middle">
    {arrow_text}
</g>
<g font-size="10" font-family="sans-serif" text-anchor="middle">
    {circle_text}
</g>
</svg>
</div>
<p>Call <em>.execute()</em> to return a list of {nodes[0].title()} and
<em>.list()</em> to return a list of {nodes[-1].title()}.</p>
"""


class NodeQueryCore(QueryCore[T_DomainModelList, T_DomainListEnd]):
    _result_cls: ClassVar[type[DomainModel]]

    def list_full(self, limit: int = DEFAULT_QUERY_LIMIT) -> T_DomainModelList:
        builder = self._create_query(limit, self._result_list_cls)
        return builder.execute(self._client)

    def _list(self, limit: int = DEFAULT_QUERY_LIMIT) -> T_DomainListEnd:
        builder = self._create_query(limit, cast(type[DomainModelList], self._result_list_cls_end))
        for step in builder[:-1]:
            step.select = None
        return builder.execute(self._client)

    def _dump_yaml(self) -> str:
        return self._create_query(DEFAULT_QUERY_LIMIT, self._result_list_cls)._dump_yaml()

    def _create_query(self, limit: int, result_list_cls: type[DomainModelList]) -> QueryBuilder:
        builder = QueryBuilder(result_list_cls)
        from_: str | None = None
        first: bool = True
        for item in self._creation_path:
            name = builder.create_name(from_)
            max_retrieve_limit = limit if first else -1
            step: QueryStep
            if isinstance(item, NodeQueryCore) and isinstance(item._expression, dm.query.NodeResultSetExpression):
                step = NodeQueryStep(
                    name=name,
                    expression=item._expression,
                    result_cls=item._result_cls,
                    max_retrieve_limit=max_retrieve_limit,
                )
                step.expression.from_ = from_
                step.expression.filter = item._assemble_filter()
                builder.append(step)
            elif isinstance(item, NodeQueryCore) and isinstance(item._expression, dm.query.EdgeResultSetExpression):
                edge_name = name
                step = EdgeQueryStep(name=edge_name, expression=item._expression, max_retrieve_limit=max_retrieve_limit)
                step.expression.from_ = from_
                builder.append(step)

                name = builder.create_name(edge_name)
                node_step = NodeQueryStep(
                    name=name,
                    expression=dm.query.NodeResultSetExpression(
                        from_=edge_name,
                        filter=item._assemble_filter(),
                    ),
                    result_cls=item._result_cls,
                )
                builder.append(node_step)
            elif isinstance(item, EdgeQueryCore):
                step = EdgeQueryStep(
                    name=name,
                    expression=cast(dm.query.EdgeResultSetExpression, item._expression),
                    result_cls=item._result_cls,
                )
                step.expression.from_ = from_
                step.expression.filter = item._assemble_filter()
                builder.append(step)
            else:
                raise TypeError(f"Unsupported query step type: {type(item._expression)}")

            first = False
            from_ = name
        return builder


class EdgeQueryCore(QueryCore[T_DomainList, T_DomainListEnd]):
    _result_cls: ClassVar[type[DomainRelation]]


class QueryStep:
    def __init__(
        self,
        name: str,
        expression: dm.query.ResultSetExpression,
        max_retrieve_limit: int = -1,
        select: dm.query.Select | None | type[_NotSetSentinel] = _NotSetSentinel,
        raw_filter: dm.Filter | None = None,
    ):
        self.name = name
        self.expression = expression
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
        self._max_retrieve_batch_limit = ACTUAL_INSTANCE_QUERY_LIMIT
        self.cursor: str | None = None
        self.total_retrieved: int = 0
        self.last_batch_count: int = 0
        self.results: list[Instance] = []

    def _default_select(self) -> dm.query.Select:
        raise NotImplementedError()

    @property
    def from_(self) -> str | None:
        return self.expression.from_

    @property
    def is_single_direct_relation(self) -> bool:
        return isinstance(self.expression, dm.query.NodeResultSetExpression) and self.expression.through is not None

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
        if self.select is None:
            raise ValueError("Cannot count total if select is not set")

        return cognite_client.data_modeling.instances.aggregate(
            self.select.sources[0].source, Count("externalId"), filter=self.raw_filter
        ).value

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r}, from={self.from_!r}, results={len(self.results)})"


class NodeQueryStep(QueryStep):
    def __init__(
        self,
        name: str,
        expression: dm.query.NodeResultSetExpression,
        result_cls: type[DomainModel],
        max_retrieve_limit: int = -1,
        select: dm.query.Select | None | type[_NotSetSentinel] = _NotSetSentinel,
        raw_filter: dm.Filter | None = None,
    ):
        self.result_cls = result_cls
        super().__init__(name, expression, max_retrieve_limit, select, raw_filter)

    def _default_select(self) -> dm.query.Select:
        return dm.query.Select([dm.query.SourceSelector(self.result_cls._view_id, ["*"])])

    def unpack(self) -> dict[dm.NodeId, DomainModel]:
        return {
            instance.as_id(): self.result_cls.from_instance(instance) for instance in cast(list[dm.Node], self.results)
        }


class EdgeQueryStep(QueryStep):
    def __init__(
        self,
        name: str,
        expression: dm.query.EdgeResultSetExpression,
        result_cls: type[DomainRelation] | None = None,
        max_retrieve_limit: int = -1,
        select: dm.query.Select | None | type[_NotSetSentinel] = _NotSetSentinel,
        raw_filter: dm.Filter | None = None,
    ):
        self.result_cls = result_cls
        super().__init__(name, expression, max_retrieve_limit, select, raw_filter)

    def _default_select(self) -> dm.query.Select:
        if self.result_cls is None:
            return dm.query.Select()
        else:
            return dm.query.Select([dm.query.SourceSelector(self.result_cls._view_id, ["*"])])

    def unpack(self) -> dict[dm.NodeId, list[dm.Edge | DomainRelation]]:
        output: dict[dm.NodeId, list[dm.Edge | DomainRelation]] = defaultdict(list)
        for edge in cast(list[dm.Edge], self.results):
            edge_source = edge.start_node if self.expression.direction == "outwards" else edge.end_node
            value = self.result_cls.from_instance(edge) if self.result_cls is not None else edge
            output[as_node_id(edge_source)].append(value)  # type: ignore[arg-type]
        return output


class QueryBuilder(list, MutableSequence[QueryStep], Generic[T_DomainModelList]):
    """This is a helper class to build and execute a query. It is responsible for
    doing the paging of the query and keeping track of the results."""

    def __init__(self, result_cls: type[T_DomainModelList] | None, steps: Collection[QueryStep] | None = None):
        super().__init__(steps or [])
        self._result_list_cls = result_cls
        self._return_step: Literal["first", "last"] = "first"

    def _reset(self):
        for expression in self:
            expression.total_retrieved = 0
            expression.cursor = None
            expression.results = []

    def _update_expression_limits(self) -> None:
        for expression in self:
            expression.update_expression_limit()

    def _build(self) -> dm.query.Query:
        with_ = {expression.name: expression.expression for expression in self}
        select = {expression.name: expression.select for expression in self if expression.select is not None}
        cursors = self._cursors

        return dm.query.Query(with_=with_, select=select, cursors=cursors)

    def _dump_yaml(self) -> str:
        return self._build().dump_yaml()

    @property
    def _cursors(self) -> dict[str, str | None]:
        return {expression.name: expression.cursor for expression in self}

    def _update(self, batch: dm.query.QueryResult):
        for expression in self:
            if expression.name not in batch:
                continue
            expression.last_batch_count = len(batch[expression.name])
            expression.total_retrieved += expression.last_batch_count
            expression.cursor = batch.cursors.get(expression.name)
            expression.results.extend(batch[expression.name].data)

    @property
    def _is_finished(self):
        return all(expression.is_finished for expression in self)

    def _reduce_max_batch_limit(self) -> bool:
        for expression in self:
            if not expression.reduce_max_batch_limit():
                return False
        return True

    def _unpack(self) -> T_DomainModelList:
        if self._result_list_cls is None:
            raise ValueError("No result class set, unable to unpack results")
        selected = [step for step in self if step.select is not None]
        if len(selected) == 0:
            return self._result_list_cls([])
        elif len(selected) == 1:
            # Validated in the append method
            if self._return_step == "first":
                selected_step = cast(NodeQueryStep, self[0])
            elif self._return_step == "last":
                selected_step = cast(NodeQueryStep, self[-1])
            else:
                raise ValueError(f"Invalid return_step: {self._return_step}")
            return self._result_list_cls(selected_step.unpack().values())
        # More than one step, we need to unpack the nodes and edges
        nodes_by_from: dict[str | None, dict[dm.NodeId, DomainModel]] = defaultdict(dict)
        edges_by_from: dict[str, dict[dm.NodeId, list[dm.Edge | DomainRelation]]] = defaultdict(dict)
        for step in reversed(self):
            # Validated in the append method
            from_ = cast(str, step.from_)
            if isinstance(step, EdgeQueryStep):
                edges_by_from[from_].update(step.unpack())
                if step.name in nodes_by_from:
                    nodes_by_from[from_].update(nodes_by_from[step.name])
                    del nodes_by_from[step.name]
            elif isinstance(step, NodeQueryStep):
                unpacked = step.unpack()
                nodes_by_from[from_].update(unpacked)
                if step.name in nodes_by_from or step.name in edges_by_from:
                    step.result_cls._update_connections(
                        unpacked,  # type: ignore[arg-type]
                        nodes_by_from.get(step.name, {}),
                        edges_by_from.get(step.name, {}),
                    )
        if self._return_step == "first":
            return self._result_list_cls(nodes_by_from[None].values())
        elif self._return_step == "last" and self[-1].from_ in nodes_by_from:
            return self._result_list_cls(nodes_by_from[self[-1].from_].values())
        elif self._return_step == "last":
            raise ValueError("Cannot return the last step when the last step is an edge query")
        else:
            raise ValueError(f"Invalid return_step: {self._return_step}")

    @overload
    def execute(self, client: CogniteClient, unpack: Literal[True] = True) -> T_DomainModelList: ...

    @overload
    def execute(self, client: CogniteClient, unpack: Literal[False]) -> None: ...

    def execute(self, client: CogniteClient, unpack: bool = True) -> T_DomainModelList | None:
        self._reset()
        query = self._build()

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
                print(f"Large query detected. Will print progress.")

        if not unpack:
            return None
        return self._unpack()

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
            if self._result_list_cls is None:
                self._return_step = "first"
            else:
                if not isinstance(__object, NodeQueryStep):
                    raise ValueError("The first step should be a NodeQueryStep")
                # If the first step is a NodeQueryStep, and matches the instance
                # in the result_list_cls we can return the result from the first step
                # Alternative is result_cls is not set, then we also assume that the first step
                if __object.result_cls is self._result_list_cls._INSTANCE:
                    self._return_step = "first"
                else:
                    # If not, we assume that the last step is the one we want to return
                    self._return_step = "last"
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
    def __getitem__(self, item: slice) -> QueryBuilder[T_DomainModelList]: ...

    def __getitem__(self, item: SupportsIndex | slice) -> QueryStep | QueryBuilder[T_DomainModelList]:
        value = super().__getitem__(item)
        if isinstance(item, slice):
            return QueryBuilder(self._result_list_cls, value)  # type: ignore[arg-type]
        return cast(QueryStep, value)


T_QueryCore = TypeVar("T_QueryCore")


class Filtering(Generic[T_QueryCore], ABC):
    def __init__(self, query: T_QueryCore, prop_path: list[str] | tuple[str, ...]):
        self._query = query
        self._prop_path = prop_path
        self._filter: dm.Filter | None = None

    def _raise_if_filter_set(self):
        if self._filter is not None:
            raise ValueError("Filter has already been set")

    def _as_filter(self) -> dm.Filter | None:
        return self._filter


class StringFilter(Filtering[T_QueryCore]):
    def equals(self, value: str) -> T_QueryCore:
        self._raise_if_filter_set()
        self._filter = dm.filters.Equals(self._prop_path, value)
        return self._query

    def prefix(self, prefix: str) -> T_QueryCore:
        self._raise_if_filter_set()
        self._filter = dm.filters.Prefix(self._prop_path, prefix)
        return self._query

    def in_(self, values: list[str]) -> T_QueryCore:
        self._raise_if_filter_set()
        self._filter = dm.filters.In(self._prop_path, values)
        return self._query


class BooleanFilter(Filtering[T_QueryCore]):
    def equals(self, value: bool) -> T_QueryCore:
        self._raise_if_filter_set()
        self._filter = dm.filters.Equals(self._prop_path, value)
        return self._query


class IntFilter(Filtering[T_QueryCore]):
    def range(self, gte: int | None, lte: int | None) -> T_QueryCore:
        self._raise_if_filter_set()
        self._filter = dm.filters.Range(self._prop_path, gte=gte, lte=lte)
        return self._query


class FloatFilter(Filtering[T_QueryCore]):
    def range(self, gte: float | None, lte: float | None) -> T_QueryCore:
        self._raise_if_filter_set()
        self._filter = dm.filters.Range(self._prop_path, gte=gte, lte=lte)
        return self._query


class TimestampFilter(Filtering[T_QueryCore]):
    def range(self, gte: datetime.datetime | None, lte: datetime.datetime | None) -> T_QueryCore:
        self._raise_if_filter_set()
        self._filter = dm.filters.Range(
            self._prop_path,
            gte=gte.isoformat(timespec="milliseconds") if gte else None,
            lte=lte.isoformat(timespec="milliseconds") if lte else None,
        )
        return self._query


class DateFilter(Filtering[T_QueryCore]):
    def range(self, gte: datetime.date | None, lte: datetime.date | None) -> T_QueryCore:
        self._raise_if_filter_set()
        self._filter = dm.filters.Range(
            self._prop_path,
            gte=gte.isoformat() if gte else None,
            lte=lte.isoformat() if lte else None,
        )
        return self._query
