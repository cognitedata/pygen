from __future__ import annotations

from collections.abc import Sequence
from typing import Literal
from cognite.client import data_modeling as dm

from omni_sub._api._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter


class ConnectionItemAOutwardsAPI(EdgeAPI):
    def list(
        self,
        from_connection_item_a: dm.NodeId | list[dm.NodeId] | None = None,
        to_connection_item_b: dm.NodeId | list[dm.NodeId] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List outward edges of a connection item a.

        Args:
            from_connection_item_a: ID of the source connection item a.
            to_connection_item_b: ID of the target connection item b.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of outward edges to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.

        Returns:
            The requested outward edges.

        Examples:

            List 5 outward edges connected to "my_connection_item_a":

                >>> from omni_sub import OmniSubClient
                >>> from cognite.client import data_modeling as dm
                >>> client = OmniSubClient()
                >>> connection_item_a = client.connection_item_a.outwards_edge.list(
                ...     dm.NodeId("my_space", "my_connection_item_a"), limit=5
                ... )

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("sp_pygen_models", "bidirectional"),
            from_connection_item_a,
            to_connection_item_b,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
