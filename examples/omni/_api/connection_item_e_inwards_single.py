from __future__ import annotations

from collections.abc import Sequence
from typing import Literal
from cognite.client import data_modeling as dm

from omni._api._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from omni.data_classes._core import DEFAULT_INSTANCE_SPACE


class ConnectionItemEInwardsSingleAPI(EdgeAPI):
    def list(
        self,
        from_connection_item_e: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_connection_item_e_space: str = DEFAULT_INSTANCE_SPACE,
        to_connection_item_d: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_connection_item_d_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List inwards single edges of a connection item e.

        Args:
            from_connection_item_e: ID of the source connection item e.
            from_connection_item_e_space: Location of the connection item es.
            to_connection_item_d: ID of the target connection item d.
            to_connection_item_d_space: Location of the connection item ds.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of inwards single edges to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.

        Returns:
            The requested inwards single edges.

        Examples:

            List 5 inwards single edges connected to "my_connection_item_e":

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> connection_item_e = client.connection_item_e.inwards_single_edge.list(
                ...     "my_connection_item_e", limit=5
                ... )

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("sp_pygen_models", "bidirectionalSingle"),
            to_connection_item_d,
            to_connection_item_d_space,
            from_connection_item_e,
            from_connection_item_e_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
