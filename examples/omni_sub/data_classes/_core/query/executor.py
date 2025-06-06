import datetime
import time
import warnings
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field

from cognite.client import CogniteClient
from cognite.client.data_classes import data_modeling as dm
from cognite.client.data_classes.aggregations import Count
from cognite.client.exceptions import CogniteAPIError

from omni_sub.data_classes._core.query.constants import (
    IN_FILTER_CHUNK_SIZE,
    INSTANCE_QUERY_LIMIT,
    MINIMUM_ESTIMATED_SECONDS_BEFORE_PRINT_PROGRESS,
    PRINT_PROGRESS_PER_N_NODES,
    SEARCH_LIMIT,
)
from omni_sub.data_classes._core.query.processing import QueryResultCleaner
from omni_sub.data_classes._core.query.step import QueryBuildStep, QueryResultStep, QueryResultStepList


class QueryReducingBatchSize(UserWarning):
    """Raised when a query is too large and the batch size must be reduced."""

    ...


@dataclass
class Progress:
    total: float | None
    _last_print: float = field(default=0.0, init=False)
    _estimated_nodes_per_second: float = field(default=0.0, init=False)
    _is_large_query: bool = field(default=False, init=False)

    def _update_nodes_per_second(self, last_node_count: int, last_execution_time: float) -> None:
        # Estimate the number of nodes per second using exponential moving average
        last_batch_nodes_per_second = last_node_count / max(last_execution_time, 1e-6)
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


@dataclass
class PaginationStatus:
    is_unlimited: bool
    max_retrieve_limit: int
    is_queryable: bool
    max_retrieve_batch_limit: int = INSTANCE_QUERY_LIMIT
    cursor: str | None = None
    total_retrieved: int = 0
    last_batch_count: int = 0

    @property
    def is_finished(self) -> bool:
        return (
            (not self.is_unlimited and self.total_retrieved >= self.max_retrieve_limit)
            or self.cursor is None
            or self.last_batch_count == 0
        )


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


class QueryExecutor:
    def __init__(
        self,
        steps: Sequence[QueryBuildStep],
        query: dm.query.Query,
        to_search: Sequence[QueryBuildStep],
        temp_select: set[str],
    ) -> None:
        step_names = set(step.name for step in steps)
        search_names = set(step.name for step in to_search)
        if not set(query.with_).issubset(step_names):
            raise ValueError("Bug in Pygen: Query step must be a subset of the query steps")
        if not search_names.issubset(step_names):
            raise ValueError("Bug in Pygen: Search step must be a subset of the query steps")
        if (search_names | set(query.with_)) != step_names:
            raise ValueError("Bug in Pygen: All steps must be either a query or a search step")
        self._steps = steps
        self._query = query
        self._to_search = to_search
        self._temp_select = temp_select
        self._status_by_name = {
            step.name: PaginationStatus(
                step.is_unlimited,
                step.max_retrieve_limit,
                step.is_queryable,
                max_retrieve_batch_limit=step.max_retrieve_batch_limit,
            )
            for step in steps
        }

    def execute_query(
        self,
        client: CogniteClient,
        remove_not_connected: bool = False,
        init_cursors: dict[str, str | None] | None = None,
    ) -> list[QueryResultStep]:
        results: dict[str, QueryResultStep] = {}
        for iterate_response in self.iterate(client, remove_not_connected, init_cursors):
            for result in iterate_response:
                if result.name in results:
                    results[result.name].results.extend(result.results)
                else:
                    results[result.name] = result
        return list(results.values())

    def iterate(
        self,
        client: CogniteClient,
        remove_not_connected: bool = False,
        init_cursors: dict[str, str | None] | None = None,
    ) -> Iterator[QueryResultStepList]:
        select_step = next((step for step in self._steps if step.select is not None), None)
        if select_step is None:
            raise ValueError("No select step found in the query")
        total = self.count_total(client, select_step)
        progress = Progress(total)
        self._query.cursors = init_cursors or self._cursors
        status = self._status_by_name[select_step.name]
        while True:
            self._update_expression_limits()
            start_query = time.time()
            try:
                batch = client.data_modeling.instances.query(self._query)
            except CogniteAPIError as e:
                if e.code == 408:
                    # Too big query, try to reduce the limit
                    if self._reduce_max_batch_limit():
                        new_limit = status.max_retrieve_batch_limit
                        warnings.warn(
                            f"Query is too large, reducing batch size to {new_limit:,}, and trying again",
                            QueryReducingBatchSize,
                            stacklevel=2,
                        )
                        continue

                raise e

            self._fetch_reverse_direct_relation_of_lists(client, self._to_search, batch)
            last_execution_time = time.time() - start_query

            for name in self._temp_select:
                batch.pop(name, None)

            self._update_pagination_status(batch)
            batch_results = self._as_results(batch)
            if remove_not_connected and len(batch_results) > 1:
                removed = QueryResultCleaner(batch_results).clean()
                for step in batch_results:
                    self._status_by_name[step.name].total_retrieved -= removed.get(step.name, 0)

            yield batch_results

            if status.is_finished:
                break

            progress.log(len(batch[select_step.name]), last_execution_time, status.total_retrieved)

            self._query.cursors = self._cursors

    def _update_expression_limits(self) -> None:
        for name, status in self._status_by_name.items():
            if name not in self._query.with_:
                continue
            # In the QueryExecutor we are only working with NodeOrEdgeResultSetExpression, so we cast it.
            expression = self._query.with_[name]
            if not isinstance(expression, dm.query.NodeOrEdgeResultSetExpression):
                raise ValueError(f"Expected NodeOrEdgeResultSetExpression for step '{name}', got {type(expression)}")
            if status.is_unlimited:
                expression.limit = status.max_retrieve_batch_limit
            else:
                expression.limit = max(
                    min(status.max_retrieve_batch_limit, status.max_retrieve_limit - status.total_retrieved), 0
                )

    @property
    def _cursors(self) -> dict[str, str | None]:
        return {name: status.cursor for name, status in self._status_by_name.items() if status.is_queryable}

    def _reduce_max_batch_limit(self) -> bool:
        for status in self._status_by_name.values():
            status.max_retrieve_batch_limit = max(1, status.max_retrieve_batch_limit // 2)
            if status.max_retrieve_batch_limit <= 1:
                return False
        return True

    @staticmethod
    def _fetch_reverse_direct_relation_of_lists(
        client: CogniteClient, to_search: Sequence[QueryBuildStep], batch: dm.query.QueryResult
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

    def _update_pagination_status(self, batch: dm.query.QueryResult) -> None:
        for name, status in self._status_by_name.items():
            if name not in batch:
                continue
            status.last_batch_count = len(batch[name])
            status.total_retrieved += status.last_batch_count
            status.cursor = batch.cursors.get(name)

    def _as_results(self, batch: dm.query.QueryResult) -> QueryResultStepList:
        results = QueryResultStepList(cursors=self._cursors)
        for step in self._steps:
            if step.name not in batch:
                continue
            results.append(QueryResultStep.from_build(batch[step.name], step))
        return results

    @staticmethod
    def count_total(cognite_client: CogniteClient, step: QueryBuildStep) -> float | None:
        if step.view_id is None:
            # Cannot count the total without a view
            return None
        try:
            return cognite_client.data_modeling.instances.aggregate(
                step.view_id, Count("externalId"), filter=step.raw_filter
            ).value
        except CogniteAPIError:
            return None
