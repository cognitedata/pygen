from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from windmill.data_classes._core import DEFAULT_INSTANCE_SPACE


class WindmillMetmastAPI(EdgeAPI):
    def list(
        self,
        from_windmill: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_windmill_space: str = DEFAULT_INSTANCE_SPACE,
        to_metmast: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_metmast_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List metmast edges of a windmill.

        Args:
            from_windmill: ID of the source windmill.
            from_windmill_space: Location of the windmills.
            to_metmast: ID of the target metmast.
            to_metmast_space: Location of the metmasts.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of metmast edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested metmast edges.

        Examples:

            List 5 metmast edges connected to "my_windmill":

                >>> from windmill import WindmillClient
                >>> client = WindmillClient()
                >>> windmill = client.windmill.metmast_edge.list("my_windmill", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("power-models", "Windmill.metmast"),
            from_windmill,
            from_windmill_space,
            to_metmast,
            to_metmast_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
