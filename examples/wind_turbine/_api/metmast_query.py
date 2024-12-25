from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from wind_turbine._api._core import (
    QueryAPI,
)
from wind_turbine.data_classes import (
    Distance,
    Metmast,
)
from wind_turbine.data_classes._core import (
    DEFAULT_QUERY_LIMIT,
    DataClassQueryBuilder,
    EdgeQueryStep,
    NodeQueryStep,
    T_DomainModelList,
)
from wind_turbine.data_classes._distance import (
    _create_distance_filter,
)
from wind_turbine.data_classes._wind_turbine import (
    _create_wind_turbine_filter,
)

if TYPE_CHECKING:
    from wind_turbine._api.wind_turbine_query import WindTurbineQueryAPI


class MetmastQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("sp_pygen_power", "Metmast", "1")

    def __init__(
        self,
        client: CogniteClient,
        builder: DataClassQueryBuilder[T_DomainModelList],
        filter_: dm.filters.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ):
        super().__init__(client, builder)
        from_ = self._builder.get_from()
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    filter=filter_,
                ),
                result_cls=Metmast,
                max_retrieve_limit=limit,
            )
        )

    def wind_turbines(
        self,
        blades: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        min_capacity: float | None = None,
        max_capacity: float | None = None,
        datasheets: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        nacelle: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        rotor: (
            str
            | tuple[str, str]
            | dm.NodeId
            | dm.DirectRelationReference
            | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference]
            | None
        ) = None,
        windfarm: str | list[str] | None = None,
        windfarm_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        min_distance_edge: float | None = None,
        max_distance_edge: float | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ) -> WindTurbineQueryAPI[T_DomainModelList]:
        """Query along the wind turbine edges of the metmast.

        Args:
            blades: The blade to filter on.
            min_capacity: The minimum value of the capacity to filter on.
            max_capacity: The maximum value of the capacity to filter on.
            datasheets: The datasheet to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            nacelle: The nacelle to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            rotor: The rotor to filter on.
            windfarm: The windfarm to filter on.
            windfarm_prefix: The prefix of the windfarm to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            min_distance_edge: The minimum value of the distance to filter on.
            max_distance_edge: The maximum value of the distance to filter on.
            external_id_prefix_edge: The prefix of the external ID to filter on.
            space_edge: The space to filter on.
            filter: (Advanced) Filter applied to node. If the filtering available in the
                above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of wind turbine edges to return.
                Defaults to 3. Set to -1, float("inf") or None to return all items.

        Returns:
            WindTurbineQueryAPI: The query API for the wind turbine.
        """
        from .wind_turbine_query import WindTurbineQueryAPI

        # from is a string as we added a node query step in the __init__ method
        from_ = cast(str, self._builder.get_from())
        edge_view = Distance._view_id
        edge_filter = _create_distance_filter(
            dm.DirectRelationReference("sp_pygen_power_enterprise", "Distance"),
            edge_view,
            min_distance=min_distance_edge,
            max_distance=max_distance_edge,
            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            EdgeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="inwards",
                ),
                result_cls=Distance,
                max_retrieve_limit=limit,
            )
        )

        view_id = WindTurbineQueryAPI._view_id
        has_data = dm.filters.HasData(views=[view_id])
        node_filer = _create_wind_turbine_filter(
            view_id,
            blades,
            min_capacity,
            max_capacity,
            datasheets,
            description,
            description_prefix,
            nacelle,
            name,
            name_prefix,
            rotor,
            windfarm,
            windfarm_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        return WindTurbineQueryAPI(self._client, self._builder, node_filer, limit)

    def query(
        self,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Returns:
            The list of the source nodes of the query.

        """
        return self._query()
