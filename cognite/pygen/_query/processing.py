import copy
import warnings
from collections import defaultdict
from collections.abc import Iterable, Sequence
from typing import Any

from cognite.client.data_classes import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import Instance

from cognite.pygen._query.step import QueryStep


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
    """Unpacks the results of a query into a list of nested dictionaries."""

    def __init__(self, builder: Sequence[QueryStep], unpack_edges: bool = False):
        self._builder = builder
        self._unpack_edges = unpack_edges

    def unpack(self) -> list[dict[str, Any]]:
        nodes_by_from: dict[str | None, list[tuple[str | None, dict[dm.NodeId, list[dict[str | None, Any]]]]]] = (
            defaultdict(list)
        )
        for step in reversed(self._builder):
            connection_property: str | None = None
            if step.connection_property:
                connection_property = step.connection_property.property

            if node_expression := step.node_expression:
                unpacked = self._unpack_node(step, node_expression, nodes_by_from.get(step.name, []))
            elif edge_expression := step.edge_expression:
                unpacked = self._unpack_edge(step, edge_expression, nodes_by_from.get(step.name, []))
            else:
                raise TypeError("Unexpected step")
            nodes_by_from[step.from_].append((connection_property, unpacked))
        # The type ignore below is incorrect, but set for now to be able to run
        # mypy. Todo: Fix this.
        return [item[0] for item in nodes_by_from[None][0][1].values()]  # type: ignore[misc]

    @classmethod
    def flatten_dump(
        cls, node: dm.Node | dm.Edge, selected_properties: set[str] | None, direct_property: str | None = None
    ) -> dict[str | None, Any]:
        """Dumps the node/edge into a flat dictionary.

        Args:
            node: The node or edge to dump.
            selected_properties: The properties to include in the dump. If None, all properties are included.
            direct_property: Assumed to be the property ID of a direct relation. If present, the value
                of this property will be converted to a NodeId or a list of NodeIds. The motivation for this is
                to be able to easily connect this node/edge to other nodes/edges in the result set.

        Returns:
            A dictionary with the properties of the node or edge

        """
        dumped = node.dump()
        dumped_properties = dumped.pop("properties", {})
        item: dict[str | None, Any] = {
            key: value for key, value in dumped.items() if selected_properties is None or key in selected_properties
        }
        for _, props_by_view_id in dumped_properties.items():
            for __, props in props_by_view_id.items():
                for key, value in props.items():
                    if key == direct_property:
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
        connections: list[tuple[str | None, dict[dm.NodeId, list[dict[str | None, Any]]]]],
    ) -> dict[dm.NodeId, list[dict[str | None, Any]]]:
        step_properties = set(step.selected_properties or []) or None
        direct_property: str | None = None
        if node_expression.through and node_expression.direction == "inwards":
            direct_property = node_expression.through.property

        unpacked_by_source: dict[dm.NodeId, list[dict[str | None, Any]]] = defaultdict(list)
        for node in step.node_results:
            node_id = node.as_id()
            dumped = self.flatten_dump(node, step_properties, direct_property)
            # Add all nodes from the subsequent steps that are connected to this node
            for connection_property, node_targets_by_source in connections:
                if node_targets := node_targets_by_source.get(node_id):
                    # Reverse direct relation or Edge
                    dumped[connection_property] = node_targets
                elif connection_property in dumped:
                    # Direct relation.
                    identifier = dumped.pop(connection_property)
                    if isinstance(identifier, dict):
                        other_id = dm.NodeId.load(identifier)
                        if other_id in node_targets_by_source:
                            dumped[connection_property] = node_targets_by_source[other_id]
                        else:
                            warnings.warn(
                                f"Node {other_id} not found in {node_targets_by_source.keys()}",
                                UserWarning,
                                stacklevel=2,
                            )
                            dumped[connection_property] = identifier
                    elif isinstance(identifier, list):
                        dumped[connection_property] = []
                        for item in identifier:
                            other_id = dm.NodeId.load(item)
                            if other_id in node_targets_by_source:
                                dumped[connection_property].extend(node_targets_by_source[other_id])
                            else:
                                warnings.warn(
                                    f"Node {other_id} not found in {node_targets_by_source.keys()}",
                                    UserWarning,
                                    stacklevel=2,
                                )
                                dumped[connection_property].append(item)
                else:
                    warnings.warn(
                        f"Property {connection_property!r} not found {node_id!r}. Expected to be in {dumped.keys()}",
                        UserWarning,
                        stacklevel=2,
                    )

            if direct_property is None:
                unpacked_by_source[node_id].append(dumped)
            else:
                reverse = dumped.pop(direct_property)
                if isinstance(reverse, dm.NodeId):
                    unpacked_by_source[reverse].append(dumped)
                elif isinstance(reverse, list):
                    for item in reverse:
                        unpacked_by_source[item].append(copy.deepcopy(dumped))
        return unpacked_by_source

    def _unpack_edge(
        self,
        step: QueryStep,
        edge_expression: dm.query.EdgeResultSetExpression,
        connections: list[tuple[str | None, dict[dm.NodeId, list[dict[str | None, Any]]]]],
    ) -> dict[dm.NodeId, list[dict[str | None, Any]]]:
        step_properties = set(step.selected_properties or []) or None
        unpacked_by_source: dict[dm.NodeId, list[dict[str | None, Any]]] = defaultdict(list)
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

            for connection_property, node_targets_by_source in connections:
                if target_node in node_targets_by_source:
                    dumped[connection_property] = node_targets_by_source[target_node]

            unpacked_by_source[source_node].append(dumped)
        return unpacked_by_source
