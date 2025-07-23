import warnings
from collections import defaultdict
from collections.abc import Iterable, Sequence
from typing import Any, Literal

from cognite.client.data_classes import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import Instance

from omni_multi.data_classes._core.query.constants import DATA_RECORD_PROPERTIES
from omni_multi.data_classes._core.query.step import QueryResultStep


class QueryResultCleaner:
    """Remove nodes and edges that are not connected through the entire query"""

    def __init__(self, steps: list[QueryResultStep]):
        self._tree = self._create_tree(steps)
        self._root = steps[0]

    @classmethod
    def _create_tree(cls, steps: list[QueryResultStep]) -> dict[str, list[QueryResultStep]]:
        tree: dict[str, list[QueryResultStep]] = defaultdict(list)
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

    def _clean(self, step: QueryResultStep, removed_by_name: dict[str, int]) -> tuple[set[dm.NodeId], str | None]:
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
    """Unpacks the results of a query into a list of nested dictionaries.

    Args:
        steps: The steps of the query to unpack.
        edges: Whether to skip, include identifier, or include the full edges in the unpacking. Note that
            this is only for edges without properties. If the edge has properties, they are always included.
            See example below for more information.
        as_data_record: If True, the created time/last updated time properties are in a nested dictionary
            called "data_record". Default is False.
        edge_type_key: The key to use for the edge type. Default is "type". In pygen generated SDKs, this is set
            to 'edge_type'.
        node_type_key: The key to use for the node type. Default is "type". In pygen generated SDKs, this is set
            to 'node_type'.
        edge_connections: How to represent the connections of edges. If "list", the connections are represented
            as a list of dictionaries. If "object", the connections are represented as a dictionary with the
            connection property as the key and the target node as the value. Default is "list".

    Example:
        Unpacking query steps including edges:

        ```python
        result = QueryUnpacker(steps, edges="include").unpack()
        print(result)
        [{
            "name": "Node A",
            "externalId": "A",
            "outwards": [{
                "type": "Edge Type",
                "node": [{
                    "name": "Node B",
                    "externalId": "B"
                }]
            }]
         }]
        ```

        Unpacking query steps, including edge identifiers:

        ```python
        result = QueryUnpacker(steps, edges="skip").unpack()
        print(result)
        [{
           "name": "Node A",
           "externalId": "A",
           "outwards": [{
               "space": "space",
               "externalId": "B"
           }]
        }]
        ```

        Unpacking query steps, but skipping the edges:

        ```python
        result = QueryUnpacker(steps, edges="skip").unpack()
        print(result)
        [{
           "name": "Node A",
           "externalId": "A",
           "outwards": [{
               "name": "Node B",
               "externalId": "B"
           }]
        }]
        ```

    """

    def __init__(
        self,
        steps: Sequence[QueryResultStep],
        edges: Literal["skip", "identifier", "include"] = "skip",
        as_data_record: bool = True,
        edge_type_key: str = "edge_type",
        node_type_key: str = "node_type",
        edge_connections: Literal["list", "object"] = "list",
    ) -> None:
        self._steps = steps
        self._edges = edges
        self._as_data_record = as_data_record
        self._edge_type_key = edge_type_key
        self._node_type_key = node_type_key
        self._edge_connections = edge_connections

    def unpack(self) -> list[dict[str, Any]]:
        if not self._steps:
            return []
        # The unpacked nodes/edges are stored in the dictionary below
        # dict[Step Name, list[Connection Property, dict[Source Node ID, list[Target Node]]]]
        # This is used for each step, to look up the connected nodes/edges.
        nodes_by_step_name: dict[str, list[tuple[str, dict[dm.NodeId, list[dict[str, Any]]]]]] = defaultdict(list)
        first_step = self._steps[0]
        output: list[dict[str, Any]] = []
        # The steps are organized in a tree structure, where each step has a reference to a previous step.
        # The unpacking is done in reverse order, starting with the last step, i.e., the leaf steps. This
        # is such that when parent steps are unpacked, they can reference the already unpacked child steps.
        for step in reversed(self._steps):
            if node_expression := step.node_expression:
                unpacked = self._unpack_node(step, node_expression, nodes_by_step_name.get(step.name, []))
            elif edge_expression := step.edge_expression:
                unpacked = self._unpack_edge(step, edge_expression, nodes_by_step_name.get(step.name, []))
            else:
                raise TypeError("Unexpected step")

            if step is first_step:
                output = [item for items in unpacked.values() for item in items]
            elif (connection_property := step.connection_property) and (step.from_ is not None):
                nodes_by_step_name[step.from_].append((connection_property.property, unpacked))
            else:
                raise ValueError(
                    f"Connection property missing in step {step!r}. This is requires for unpacking"
                    "for all steps except the first step."
                )

        return output

    @classmethod
    def flatten_dump(
        cls,
        node: dm.Node | dm.Edge,
        selected_properties: set[str] | None,
        direct_property: str | None = None,
        as_data_record: bool = False,
        type_key: str = "type",
    ) -> dict[str, Any]:
        """Dumps the node/edge into a flat dictionary.

        Args:
            node: The node or edge to dump.
            selected_properties: The properties to include in the dump. If None, all properties are included.
            direct_property: Assumed to be the property ID of a direct relation. If present, the value
                of this property will be converted to a NodeId or a list of NodeIds. The motivation for this is
                to be able to easily connect this node/edge to other nodes/edges in the result set.
            as_data_record: If True, node properties are dumped as data records. Default is False.
            type_key: The key to use for the type. Default is "type".

        Returns:
            A dictionary with the properties of the node or edge

        """
        dumped = node.dump()
        dumped_properties = dumped.pop("properties", {})
        if "type" in dumped:
            dumped[type_key] = dumped.pop("type")

        item: dict[str, Any] = {
            key: value for key, value in dumped.items() if selected_properties is None or key in selected_properties
        }
        if as_data_record:
            data_record: dict[str, Any] = {}
            for key in list(item.keys()):
                if key in DATA_RECORD_PROPERTIES:
                    data_record[key] = item.pop(key)
            if data_record:
                item["data_record"] = data_record

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
        step: QueryResultStep,
        node_expression: dm.query.NodeResultSetExpression,
        connections: list[tuple[str, dict[dm.NodeId, list[dict[str, Any]]]]],
    ) -> dict[dm.NodeId, list[dict[str, Any]]]:
        step_properties = set(step.selected_properties or []) or None
        direct_property: str | None = None
        if node_expression.through and node_expression.direction == "inwards":
            direct_property = node_expression.through.property

        unpacked_by_source: dict[dm.NodeId, list[dict[str, Any]]] = defaultdict(list)
        for node in step.node_results:
            node_id = node.as_id()
            dumped = self.flatten_dump(
                node, step_properties, direct_property, self._as_data_record, self._node_type_key
            )
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

            if direct_property is None:
                unpacked_by_source[node_id].append(dumped)
            else:
                reverse = dumped.pop(direct_property)
                if isinstance(reverse, dm.NodeId):
                    unpacked_by_source[reverse].append(dumped)
                elif isinstance(reverse, list):
                    for item in reverse:
                        unpacked_by_source[item].append(dumped)
        return unpacked_by_source

    def _unpack_edge(
        self,
        step: QueryResultStep,
        edge_expression: dm.query.EdgeResultSetExpression,
        connections: list[tuple[str, dict[dm.NodeId, list[dict[str, Any]]]]],
    ) -> dict[dm.NodeId, list[dict[str, Any]]]:
        step_properties = set(step.selected_properties or []) or None
        unpacked_by_source: dict[dm.NodeId, list[dict[str, Any]]] = defaultdict(list)
        is_leaf_step = len(connections) == 0
        for edge in step.edge_results:
            start_node, end_node = dm.NodeId.load(edge.start_node.dump()), dm.NodeId.load(edge.end_node.dump())  # type: ignore[arg-type]
            if edge_expression.direction == "outwards":
                source_node, target_node = start_node, end_node
            else:
                source_node, target_node = end_node, start_node
            # step.view_id means that the edge has properties
            dumped_edge: dict[str, Any]
            if self._edges == "include" or step.view_id:
                dumped_edge = self.flatten_dump(
                    edge, step_properties, as_data_record=self._as_data_record, type_key=self._edge_type_key
                )
                self._append_connections(dumped_edge, connections, target_node, edge.as_id())
                dumped = [dumped_edge]
            elif self._edges == "identifier":
                dumped_edge = edge.as_id().dump(include_instance_type=False)
                self._append_connections(dumped_edge, connections, target_node, edge.as_id())
                dumped = [dumped_edge]
            elif self._edges == "skip" and is_leaf_step:
                dumped = [target_node.dump(include_instance_type=False)]
            elif self._edges == "skip":
                # Skipping the edge, instead adding the target node to the source node
                # such that the target node(s) can be connected to the source node.
                for _, node_targets_by_source in connections:
                    if target_node in node_targets_by_source:
                        unpacked_by_source[source_node].extend(node_targets_by_source[target_node])
                continue
            else:
                raise ValueError(f"Unexpected value for edges: {self._edges}")
            unpacked_by_source[source_node].extend(dumped)
        return unpacked_by_source

    def _append_connections(
        self,
        dumped_edge: dict[str, Any],
        connections: list[tuple[str, dict[dm.NodeId, list[dict[str, Any]]]]],
        target_node: dm.NodeId,
        edge_id: dm.EdgeId,
    ) -> None:
        for connection_property, node_targets_by_source in connections:
            if dumped_target_node := node_targets_by_source.get(target_node):
                if self._edge_connections == "list":
                    dumped_edge[connection_property] = dumped_target_node
                elif self._edge_connections == "object":
                    if len(dumped_target_node) != 1:
                        raise ValueError(
                            f"Expected exactly one target node for connection property '{connection_property}'"
                            f" for edge {edge_id}. Found: {len(dumped_target_node)}"
                        )
                    dumped_edge[connection_property] = dumped_target_node[0]
                else:
                    raise ValueError(
                        f"Unexpected value for edge_connections: {self._edge_connections}. Expected 'list' or 'object'."
                    )
