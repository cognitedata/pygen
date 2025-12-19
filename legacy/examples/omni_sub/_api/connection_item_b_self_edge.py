from __future__ import annotations

from collections.abc import Sequence
from typing import Literal
from cognite.client import data_modeling as dm

from omni_sub._api._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter


class ConnectionItemBSelfEdgeAPI(EdgeAPI):
    def list(
        self,
        from_connection_item_b: dm.NodeId | list[dm.NodeId] | None = None,
        to_connection_item_b: dm.NodeId | list[dm.NodeId] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List self edge edges of a connection item b.

        Args:
            from_connection_item_b: ID of the source connection item b.
            to_connection_item_b: ID of the target connection item b.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of self edge edges to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.

        Returns:
            The requested self edge edges.

        Examples:

            List 5 self edge edges connected to "my_connection_item_b":

                >>> from omni_sub import OmniSubClient
                >>> from cognite.client import data_modeling as dm
                >>> client = OmniSubClient()
                >>> connection_item_b = client.connection_item_b.self_edge_edge.list(
                ...     dm.NodeId("my_space", "my_connection_item_a"), limit=5
                ... )

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("sp_pygen_models", "reflexive"),
            from_connection_item_b,
            to_connection_item_b,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
