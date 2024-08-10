from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from omni_pydantic_v1.data_classes._core import DEFAULT_INSTANCE_SPACE


class ConnectionItemEDirectReverseSingleAPI(EdgeAPI):
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
        """List direct reverse single edges of a connection item e.

        Args:
            from_connection_item_e: ID of the source connection item e.
            from_connection_item_e_space: Location of the connection item es.
            to_connection_item_d: ID of the target connection item d.
            to_connection_item_d_space: Location of the connection item ds.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of direct reverse single edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested direct reverse single edges.

        Examples:

            List 5 direct reverse single edges connected to "my_connection_item_e":

                >>> from omni_pydantic_v1 import OmniClient
                >>> client = OmniClient()
                >>> connection_item_e = client.connection_item_e.direct_reverse_single_edge.list("my_connection_item_e", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("", ""),
            from_connection_item_e,
            from_connection_item_e_space,
            to_connection_item_d,
            to_connection_item_d_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
