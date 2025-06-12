import sys
from collections.abc import Collection, Iterable, Iterator, MutableSequence
from typing import (
    SupportsIndex,
    cast,
    overload,
)

from cognite.client.data_classes import data_modeling as dm
from cognite.client.data_classes.data_modeling.query import NodeOrEdgeResultSetExpression

from omni_sub.data_classes._core.query.executor import QueryExecutor
from omni_sub.data_classes._core.query.step import QueryBuildStep

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class QueryBuilder(list, MutableSequence[QueryBuildStep]):
    """This is a helper class to build a query. It is used to gather all steps in
    a query and build the DMS query from it."""

    def __init__(self, steps: Collection[QueryBuildStep] | None = None):
        super().__init__(steps or [])

    def _build(self) -> tuple[dm.query.Query, list[QueryBuildStep], set[str]]:
        # We serialize and deserialize query steps to get a copy as we will modify the
        # cursor and limit in the execution.
        # The cast is needed as mypy does not understand that the NodeOrEdgeResultSetExpression is
        # the parent of NodeSetExpression and EdgeSetExpression.
        with_ = {
            step.name: cast(
                NodeOrEdgeResultSetExpression, dm.query.NodeOrEdgeResultSetExpression._load(step.expression.dump())
            )
            for step in self
            if step.is_queryable
        }
        select = {step.name: step.select for step in self if step.select is not None and step.is_queryable}
        step_by_name = {step.name: step for step in self}
        search: list[QueryBuildStep] = []
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
        # MyPy requires with to be an invariant mapping. We do not control the query clas in the SDK,
        # so we use a cast here.
        return dm.query.Query(with_=with_, select=select), search, temporary_select  # type: ignore[arg-type]

    def _dump_yaml(self) -> str:
        return self._build()[0].dump_yaml()

    def build(self) -> QueryExecutor:
        query, to_search, temp_select = self._build()

        if not self:
            raise ValueError("No query steps to execute")

        return QueryExecutor(self, query, to_search, temp_select)

    def get_from(self) -> str | None:
        if len(self) == 0:
            return None
        return self[-1].name

    def create_name(self, from_: str | None) -> str:
        if from_ is None:
            return "0"
        return f"{from_}_{len(self)}"

    def append(self, __object: QueryBuildStep, /) -> None:
        # Extra validation to ensure all assumptions are met
        if len(self) == 0:
            if __object.from_ is not None:
                raise ValueError("The first step should not have a 'from_' value")
        else:
            if __object.from_ is None:
                raise ValueError("The 'from_' value should be set")
        super().append(__object)

    def extend(self, __iterable: Iterable[QueryBuildStep], /) -> None:
        for item in __iterable:
            self.append(item)

    # The implementations below are to get proper type hints
    def __iter__(self) -> Iterator[QueryBuildStep]:
        return super().__iter__()

    @overload
    def __getitem__(self, item: SupportsIndex, /) -> QueryBuildStep: ...

    @overload
    def __getitem__(self, item: slice, /) -> Self: ...

    def __getitem__(self, item: SupportsIndex | slice, /) -> QueryBuildStep | Self:
        value = super().__getitem__(item)
        if isinstance(item, slice):
            return QueryBuilder(value)  # type: ignore[arg-type, return-value]
        return cast(QueryBuildStep, value)
