from __future__ import annotations

from collections.abc import Sequence
from typing import Literal
from cognite.client import data_modeling as dm

from wind_turbine.data_classes import (
    Distance,
    DistanceList,
    DistanceWrite,
)
from wind_turbine.data_classes._distance import _create_distance_filter

from wind_turbine._api._core import DEFAULT_LIMIT_READ, EdgePropertyAPI
from wind_turbine.data_classes._core import DEFAULT_INSTANCE_SPACE


class WindTurbineMetmastAPI(EdgePropertyAPI):
    _view_id = dm.ViewId("sp_pygen_power", "Distance", "1")
    _class_type = Distance
    _class_write_type = DistanceWrite
    _class_list = DistanceList

    def list(
        self,
        from_wind_turbine: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        from_wind_turbine_space: str = DEFAULT_INSTANCE_SPACE,
        to_metmast: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        to_metmast_space: str = DEFAULT_INSTANCE_SPACE,
        min_distance: float | None = None,
        max_distance: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit=DEFAULT_LIMIT_READ,
    ) -> DistanceList:
        """List metmast edges of a wind turbine.

        Args:
            from_wind_turbine: ID of the source wind turbine.
            from_wind_turbine_space: Location of the wind turbines.
            to_metmast: ID of the target metmast.
            to_metmast_space: Location of the metmasts.
            min_distance: The minimum value of the distance to filter on.
            max_distance: The maximum value of the distance to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of metmast edges to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.

        Returns:
            The requested metmast edges.

        Examples:

            List 5 metmast edges connected to "my_wind_turbine":

                >>> from wind_turbine import WindTurbineClient
                >>> client = WindTurbineClient()
                >>> wind_turbine = client.wind_turbine.metmast_edge.list(
                ...     "my_wind_turbine", limit=5
                ... )

        """
        filter_ = _create_distance_filter(
            dm.DirectRelationReference("sp_pygen_power_enterprise", "Distance"),
            self._view_id,
            from_wind_turbine,
            from_wind_turbine_space,
            to_metmast,
            to_metmast_space,
            min_distance,
            max_distance,
            external_id_prefix,
            space,
        )
        return self._list(filter_=filter_, limit=limit)
