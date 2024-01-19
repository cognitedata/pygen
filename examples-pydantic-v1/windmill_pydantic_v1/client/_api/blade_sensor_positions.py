from __future__ import annotations


from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, EdgeAPI, _create_edge_filter
from windmill_pydantic_v1.client.data_classes._core import DEFAULT_INSTANCE_SPACE


class BladeSensorPositionsAPI(EdgeAPI):
    def list(
        self,
        from_blade: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_blade_space: str = DEFAULT_INSTANCE_SPACE,
        to_sensor_position: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_sensor_position_space: str = DEFAULT_INSTANCE_SPACE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> dm.EdgeList:
        """List sensor position edges of a blade.

        Args:
            from_blade: ID of the source blade.
            from_blade_space: Location of the blades.
            to_sensor_position: ID of the target sensor position.
            to_sensor_position_space: Location of the sensor positions.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of sensor position edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            The requested sensor position edges.

        Examples:

            List 5 sensor position edges connected to "my_blade":

                >>> from windmill_pydantic_v1.client import WindmillClient
                >>> client = WindmillClient()
                >>> blade = client.blade.sensor_positions_edge.list("my_blade", limit=5)

        """
        filter_ = _create_edge_filter(
            dm.DirectRelationReference("power-models", "Blade.sensor_positions"),
            from_blade,
            from_blade_space,
            to_sensor_position,
            to_sensor_position_space,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
