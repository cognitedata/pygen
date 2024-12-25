import copy
import warnings
from collections import defaultdict
from collections.abc import Iterable, Sequence
from typing import Any

from cognite.client.data_classes import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import Instance

from omni_sub.data_classes._core.query.step import QueryStep


class QueryResultCleaner:
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

    def clean(self) -> dict[str, int]:
        removed_by_name: dict[str, int] = defaultdict(int)
        self._clean(self._root, removed_by_name)
        return dict(removed_by_name)

    @staticmethod
    def as_node_id(direct_relation: dm.DirectRelationReference | dict[str, str]) -> dm.NodeId:
        if isinstance(direct_relation, dict):
            return dm.NodeId(direct_relation["space"], direct_relation["externalId"])

        return dm.NodeId(direct_relation.space, direct_relation.external_id)

    def _clean(self, step: QueryStep, removed_by_name: dict[str, int]) -> tuple[set[dm.NodeId], str | None]:
        if step.name not in self._tree:
            # Leaf Node
            # Nothing to clean, just return the node ids with the connection property
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
            child_ids, property_id = self._clean(child, removed_by_name)
            if property_id not in expected_ids_by_property:
                expected_ids_by_property[property_id] = child_ids
            else:
                expected_ids_by_property[property_id] |= child_ids

        if step.node_expression is not None:
            filtered_results: list[Instance] = []
            for node in step.node_results:
                if self._is_connected_node(node, expected_ids_by_property):
                    filtered_results.append(node)
                else:
                    removed_by_name[step.name] += 1
            step.results = filtered_results
            direct_relation = None if step.node_expression.through is None else step.node_expression.through.property
            return {node.as_id() for node in step.node_results}, direct_relation

        if step.edge_expression:
            if len(expected_ids_by_property) > 1 or None not in expected_ids_by_property:
                raise RuntimeError(f"Invalid state of {type(self).__name__}")
            expected_ids = expected_ids_by_property[None]
            before = len(step.results)
            if step.edge_expression.direction == "outwards":
                step.results = [edge for edge in step.edge_results if self.as_node_id(edge.end_node) in expected_ids]
                connected_node_ids = {self.as_node_id(edge.start_node) for edge in step.edge_results}
            else:  # inwards
                step.results = [edge for edge in step.edge_results if self.as_node_id(edge.start_node) in expected_ids]
                connected_node_ids = {self.as_node_id(edge.end_node) for edge in step.edge_results}
            removed_by_name[step.name] += before - len(step.results)
            return connected_node_ids, None

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


class QueryUnpacker:
    def __init__(self, builder: Sequence[QueryStep]):
        self._builder = builder

    def unpack(self) -> list[dict[str, Any]]:
        nodes_by_from: dict[str | None, list[tuple[str | None, dict[dm.NodeId, list[dict[str | None, Any]]]]]] = (
            defaultdict(list)
        )
        for step in reversed(self._builder):
            source_property: str | None = None
            if step.view_property:
                source_property = step.view_property.property
            if node_expression := step.node_expression:
                unpacked = self._unpack_node(step, node_expression, nodes_by_from)
                nodes_by_from[step.from_].append((source_property, unpacked))
            elif edge_expression := step.edge_expression:
                step_properties = set(step.selected_properties or [])
                unpacked_edge: dict[dm.NodeId, list[dict[str | None, Any]]] = defaultdict(list)
                for edge in step.edge_results:
                    start_node = dm.NodeId.load(edge.start_node.dump())  # type: ignore[arg-type]
                    end_node = dm.NodeId.load(edge.end_node.dump())  # type: ignore[arg-type]
                    dumped = self.flatten_dump(edge, step_properties)
                    if edge_expression.direction == "outwards":
                        source_node = start_node
                        target_node = end_node
                    else:
                        source_node = end_node
                        target_node = start_node
                    for nested_prop, nested_by_id in nodes_by_from.get(step.name, []):
                        if target_node in nested_by_id:
                            dumped[nested_prop] = nested_by_id[target_node]

                    unpacked_edge[source_node].append(dumped)
                nodes_by_from[step.from_].append((source_property, unpacked_edge))
            else:
                raise TypeError("Unexpected step")
        # The type ignore below is incorrect, but set for now to be able to run
        # mypy. Todo: Fix this.
        return [item[0] for item in nodes_by_from[None][0][1].values()]  # type: ignore[misc]

    @classmethod
    def flatten_dump(
        cls, node: dm.Node | dm.Edge, selected_properties: set[str] | None, connection_property: str | None = None
    ) -> dict[str | None, Any]:
        dumped = node.dump()
        dumped_properties = dumped.pop("properties", {})
        item: dict[str | None, Any] = {
            key: value for key, value in dumped.items() if selected_properties is None or key in selected_properties
        }
        for _, props_by_view_id in dumped_properties.items():
            for __, props in props_by_view_id.items():
                for key, value in props.items():
                    if key == connection_property:
                        if isinstance(value, dict):
                            item[key] = dm.NodeId.load(value)
                        elif isinstance(value, list):
                            item[key] = [dm.NodeId.load(item) for item in value]
                        else:
                            raise TypeError(f"Unexpected connection property value: {value}")
                    elif selected_properties is None or key in selected_properties:
                        item[key] = value
        return item

    def _unpack_node(
        self,
        step: QueryStep,
        node_expression: dm.query.NodeResultSetExpression,
        results_by_from: dict[str | None, list[tuple[str | None, dict[dm.NodeId, list[dict[str | None, Any]]]]]],
    ) -> dict[dm.NodeId, list[dict[str | None, Any]]]:
        step_properties = set(step.selected_properties or [])
        connection_property: str | None = None
        if node_expression.through and node_expression.direction == "inwards":
            connection_property = node_expression.through.property
        unpacked: dict[dm.NodeId, list[dict[str | None, Any]]] = defaultdict(list)
        for node in step.node_results:
            node_id = node.as_id()
            dumped = self.flatten_dump(node, step_properties, connection_property)
            for nested_prop, nested_by_id in results_by_from.get(step.name, []):
                if neste_item := nested_by_id.get(node_id):
                    # Reverse or Edge
                    dumped[nested_prop] = neste_item
                elif nested_prop in dumped:
                    # Direct relation
                    identifier = dumped.pop(nested_prop)
                    if isinstance(identifier, dict):
                        other_id = dm.NodeId.load(identifier)
                        if other_id in nested_by_id:
                            dumped[nested_prop] = nested_by_id[other_id]
                    elif isinstance(identifier, list):
                        dumped[nested_prop] = []
                        for item in identifier:
                            other_id = dm.NodeId.load(item)
                            if other_id in nested_by_id:
                                dumped[nested_prop].extend(nested_by_id[other_id])
                else:
                    warnings.warn(
                        f"Nested property {nested_prop} not found in {dumped.keys()}", UserWarning, stacklevel=2
                    )

            if connection_property is None:
                unpacked[node_id].append(dumped)
            else:
                reverse = dumped.pop(connection_property)
                if isinstance(reverse, dm.NodeId):
                    unpacked[reverse].append(dumped)
                elif isinstance(reverse, list):
                    for item in reverse:
                        unpacked[item].append(copy.deepcopy(dumped))
        return unpacked
