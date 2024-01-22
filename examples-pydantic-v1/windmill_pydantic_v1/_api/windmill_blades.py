from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from windmill_pydantic_v1.data_classes._core import DEFAULT_INSTANCE_SPACE


class WindmillBladesAPI(EdgeAPI):
    def list(
        self,
        from_windmill: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_windmill_space: str = DEFAULT_INSTANCE_SPACE,
        to_blade: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_blade_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List blade edges of a windmill.

        Args:
            from_windmill: ID of the source windmill.
            from_windmill_space: Location of the windmills.
            to_blade: ID of the target blade.
            to_blade_space: Location of the blades.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of blade edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested blade edges.

        Examples:

            List 5 blade edges connected to "my_windmill":

                >>> from windmill_pydantic_v1 import WindmillClient
                >>> client = WindmillClient()
                >>> windmill = client.windmill.blades_edge.list("my_windmill", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("power-models", "Windmill.blades"),
            from_windmill,
            from_windmill_space,
            to_blade,
            to_blade_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
