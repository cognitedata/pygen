from __future__ import annotations

from collections.abc import Sequence
from typing import Literal
from cognite.client import data_modeling as dm

from omni._api._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from omni.data_classes._core import DEFAULT_INSTANCE_SPACE


class ConnectionItemDOutwardsSingleAPI(EdgeAPI):
    def list(
        self,
        from_connection_item_d: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_connection_item_d_space: str = DEFAULT_INSTANCE_SPACE,
        to_connection_item_e: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_connection_item_e_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List outwards single edges of a connection item d.

        Args:
            from_connection_item_d: ID of the source connection item d.
            from_connection_item_d_space: Location of the connection item ds.
            to_connection_item_e: ID of the target connection item e.
            to_connection_item_e_space: Location of the connection item es.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of outwards single edges to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.

        Returns:
            The requested outwards single edges.

        Examples:

            List 5 outwards single edges connected to "my_connection_item_d":

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> connection_item_d = client.connection_item_d.outwards_single_edge.list(
                ...     "my_connection_item_d", limit=5
                ... )

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("sp_pygen_models", "bidirectionalSingle"),
            from_connection_item_d,
            from_connection_item_d_space,
            to_connection_item_e,
            to_connection_item_e_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
