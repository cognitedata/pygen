from __future__ import annotations

from collections.abc import Sequence
from typing import Literal
from cognite.client import data_modeling as dm

from omni._api._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from omni.data_classes._core import DEFAULT_INSTANCE_SPACE


class ConnectionItemCNodeConnectionItemAAPI(EdgeAPI):
    def list(
        self,
        from_connection_item_c_node: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_connection_item_c_node_space: str = DEFAULT_INSTANCE_SPACE,
        to_connection_item_a: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_connection_item_a_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List connection item a edges of a connection item c node.

        Args:
            from_connection_item_c_node: ID of the source connection item c node.
            from_connection_item_c_node_space: Location of the connection item c nodes.
            to_connection_item_a: ID of the target connection item a.
            to_connection_item_a_space: Location of the connection item as.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item a edges to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.

        Returns:
            The requested connection item a edges.

        Examples:

            List 5 connection item a edges connected to "my_connection_item_c_node":

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> connection_item_c_node = client.connection_item_c_node.connection_item_a_edge.list(
                ...     "my_connection_item_c_node", limit=5
                ... )

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("sp_pygen_models", "unidirectional"),
            from_connection_item_c_node,
            from_connection_item_c_node_space,
            to_connection_item_a,
            to_connection_item_a_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
