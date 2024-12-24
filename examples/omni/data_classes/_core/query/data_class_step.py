from __future__ import annotations

from collections import defaultdict
from collections.abc import Collection
from typing import cast, Generic, Literal

from cognite.client import data_modeling as dm

from omni.data_classes._core.query.builder import QueryBuilder
from omni.data_classes._core.query.step import QueryStep
from omni.data_classes._core.base import DomainModel, DomainRelation, T_DomainModelList
from omni.data_classes._core.constants import DEFAULT_INSTANCE_SPACE
from omni.data_classes._core.query.constants import NotSetSentinel
from omni.data_classes._core.helpers import as_node_id


class NodeQueryStep(QueryStep):
    def __init__(
        self,
        name: str,
        expression: dm.query.NodeResultSetExpression,
        result_cls: type[DomainModel],
        max_retrieve_limit: int = -1,
        select: dm.query.Select | None | type[NotSetSentinel] = NotSetSentinel,
        raw_filter: dm.Filter | None = None,
        connection_type: Literal["reverse-list"] | None = None,
    ):
        self.result_cls = result_cls
        super().__init__(name, expression, result_cls._view_id, max_retrieve_limit, select, raw_filter, connection_type)

    def unpack(self) -> dict[dm.NodeId | str, DomainModel]:
        return {
            (
                instance.as_id() if instance.space != DEFAULT_INSTANCE_SPACE else instance.external_id
            ): self.result_cls.from_instance(instance)
            for instance in cast(list[dm.Node], self.results)
        }

    @property
    def node_results(self) -> list[dm.Node]:
        return cast(list[dm.Node], self.results)

    @property
    def node_expression(self) -> dm.query.NodeResultSetExpression:
        return cast(dm.query.NodeResultSetExpression, self.expression)


class EdgeQueryStep(QueryStep):
    def __init__(
        self,
        name: str,
        expression: dm.query.EdgeResultSetExpression,
        result_cls: type[DomainRelation] | None = None,
        max_retrieve_limit: int = -1,
        select: dm.query.Select | None | type[NotSetSentinel] = NotSetSentinel,
        raw_filter: dm.Filter | None = None,
    ):
        self.result_cls = result_cls
        view_id = result_cls._view_id if result_cls is not None else None
        super().__init__(name, expression, view_id, max_retrieve_limit, select, raw_filter, None)

    def unpack(self) -> dict[dm.NodeId, list[dm.Edge | DomainRelation]]:
        output: dict[dm.NodeId, list[dm.Edge | DomainRelation]] = defaultdict(list)
        for edge in cast(list[dm.Edge], self.results):
            edge_source = edge.start_node if self.expression.direction == "outwards" else edge.end_node
            value = self.result_cls.from_instance(edge) if self.result_cls is not None else edge
            output[as_node_id(edge_source)].append(value)  # type: ignore[arg-type]
        return output

    @property
    def edge_results(self) -> list[dm.Edge]:
        return cast(list[dm.Edge], self.results)

    @property
    def edge_expression(self) -> dm.query.EdgeResultSetExpression:
        return cast(dm.query.EdgeResultSetExpression, self.expression)


class DataClassQueryBuilder(QueryBuilder, Generic[T_DomainModelList]):
    """This is a helper class to build and execute a query. It is responsible for
    doing the paging of the query and keeping track of the results."""

    def __init__(
        self,
        result_cls: type[T_DomainModelList] | None,
        steps: Collection[QueryStep] | None = None,
        return_step: Literal["first", "last"] | None = None,
    ):
        super().__init__(steps or [])
        self._result_list_cls = result_cls
        self._return_step: Literal["first", "last"] | None = return_step

    def unpack(self) -> T_DomainModelList:
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
        nodes_by_from: dict[str | None, dict[dm.NodeId | str, DomainModel]] = defaultdict(dict)
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
                nodes_by_from[from_].update(unpacked)  # type: ignore[arg-type]
                if step.name in nodes_by_from or step.name in edges_by_from:
                    step.result_cls._update_connections(
                        unpacked,  # type: ignore[arg-type]
                        nodes_by_from.get(step.name, {}),  # type: ignore[arg-type]
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

    def append(self, __object: QueryStep, /) -> None: #type: ignore[override]
        # Extra validation to ensure all assumptions are met
        if len(self) == 0:
            if __object.from_ is not None:
                raise ValueError("The first step should not have a 'from_' value")
            if self._result_list_cls is None:
                if self._return_step is None:
                    self._return_step = "first"
            else:
                if not isinstance(__object, NodeQueryStep):
                    raise ValueError("The first step should be a NodeQueryStep")
                # If the first step is a NodeQueryStep, and matches the instance
                # in the result_list_cls we can return the result from the first step
                # Alternative is result_cls is not set, then we also assume that the first step
                if self._return_step is None:
                    if __object.result_cls is self._result_list_cls._INSTANCE:
                        self._return_step = "first"
                    else:
                        # If not, we assume that the last step is the one we want to return
                        self._return_step = "last"
        else:
            if __object.from_ is None:
                raise ValueError("The 'from_' value should be set")
        super().append(__object)
