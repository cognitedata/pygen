import datetime
import sys
import time
import warnings
from collections.abc import Collection, Iterable, Iterator, MutableSequence, Sequence
from dataclasses import dataclass, field
from typing import (
    SupportsIndex,
    cast,
    overload,
)

from cognite.client import CogniteClient
from cognite.client.data_classes import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import Instance
from cognite.client.exceptions import CogniteAPIError

from cognite.pygen._query.constants import (
    IN_FILTER_CHUNK_SIZE,
    MINIMUM_ESTIMATED_SECONDS_BEFORE_PRINT_PROGRESS,
    PRINT_PROGRESS_PER_N_NODES,
    SEARCH_LIMIT,
)
from cognite.pygen._query.processing import QueryResultCleaner
from cognite.pygen._query.step import QueryStep

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


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


@dataclass
class Progress:
    total: float | None
    _last_print: float = field(default=0.0, init=False)
    _estimated_nodes_per_second: float = field(default=0.0, init=False)
    _is_large_query: bool = field(default=False, init=False)

    def _update_nodes_per_second(self, last_node_count: int, last_execution_time: float) -> None:
        # Estimate the number of nodes per second using exponential moving average
        last_batch_nodes_per_second = last_node_count / last_execution_time
        if self._estimated_nodes_per_second == 0.0:
            self._estimated_nodes_per_second = last_batch_nodes_per_second
        else:
            self._estimated_nodes_per_second = (
                0.1 * last_batch_nodes_per_second + 0.9 * self._estimated_nodes_per_second
            )

    def log(self, last_node_count: int, last_execution_time: float, total_retrieved: int) -> None:
        if self.total is None:
            return
        self._update_nodes_per_second(last_node_count, last_execution_time)
        # Estimate the time to completion
        remaining_nodes = self.total - total_retrieved
        remaining_time = remaining_nodes / self._estimated_nodes_per_second
        if self._is_large_query and (total_retrieved - self._last_print) > PRINT_PROGRESS_PER_N_NODES:
            estimate = datetime.timedelta(seconds=round(remaining_time, 0))
            print(
                f"Progress: {total_retrieved:,}/{self.total:,} nodes retrieved. "
                f"Estimated time to completion: {estimate}"
            )
            self._last_print = total_retrieved
        if self._is_large_query is False and remaining_time > MINIMUM_ESTIMATED_SECONDS_BEFORE_PRINT_PROGRESS:
            self._is_large_query = True
            print("Large query detected. Will print progress.")


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

        select_step = next((step for step in self if step.select is not None), None)
        if select_step is None:
            raise ValueError("No select step found in the query")

        total = select_step.count_total(client)

        progress = Progress(total)
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
                    new_limit = select_step._max_retrieve_batch_limit
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
            if remove_not_connected and len(self) > 1:
                removed = QueryResultCleaner(self).clean()
                for step in self:
                    step.total_retrieved -= removed.get(step.name, 0)

            if select_step.is_finished:
                break

            progress.log(len(batch[select_step.name]), last_execution_time, select_step.total_retrieved)

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
    def __getitem__(self, item: SupportsIndex, /) -> QueryStep: ...

    @overload
    def __getitem__(self, item: slice, /) -> Self: ...

    def __getitem__(self, item: SupportsIndex | slice, /) -> QueryStep | Self:
        value = super().__getitem__(item)
        if isinstance(item, slice):
            return QueryBuilder(value)  # type: ignore[arg-type, return-value]
        return cast(QueryStep, value)
