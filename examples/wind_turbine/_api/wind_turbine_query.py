from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

from cognite.client import data_modeling as dm, CogniteClient

from wind_turbine.data_classes import (
    DomainModelCore,
    WindTurbine,
    Distance,
    Nacelle,
    Rotor,
)
from wind_turbine.data_classes._core import (
    DEFAULT_QUERY_LIMIT,
    ViewPropertyId,
    T_DomainModel,
    T_DomainModelList,
    QueryBuilder,
    QueryStep,
)
from wind_turbine.data_classes._metmast import (
    _create_metmast_filter,
)
from wind_turbine._api._core import (
    QueryAPI,
    _create_edge_filter,
)

from wind_turbine.data_classes._distance import (
    _create_distance_filter,
)

if TYPE_CHECKING:
    from wind_turbine._api.metmast_query import MetmastQueryAPI


class WindTurbineQueryAPI(QueryAPI[T_DomainModel, T_DomainModelList]):
    _view_id = dm.ViewId("sp_pygen_power", "WindTurbine", "1")

    def __init__(
        self,
        client: CogniteClient,
        builder: QueryBuilder,
        result_cls: type[T_DomainModel],
        result_list_cls: type[T_DomainModelList],
        connection_property: ViewPropertyId | None = None,
        filter_: dm.filters.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ):
        super().__init__(client, builder, result_cls, result_list_cls)
        from_ = self._builder.get_from()
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    filter=filter_,
                ),
                max_retrieve_limit=limit,
                view_id=self._view_id,
                connection_property=connection_property,
            )
        )

    def metmast(
        self,
        min_position: float | None = None,
        max_position: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        min_distance_edge: float | None = None,
        max_distance_edge: float | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        retrieve_nacelle: bool = False,
        retrieve_rotor: bool = False,
    ) -> MetmastQueryAPI[T_DomainModel, T_DomainModelList]:
        """Query along the metmast edges of the wind turbine.

        Args:
            min_position:
            max_position:
            external_id_prefix:
            space:
            min_distance_edge:
            max_distance_edge:
            external_id_prefix_edge:
            space_edge:
            filter: (Advanced) Filter applied to node. If the filtering available in the
                above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of metmast edges to return.
                Defaults to 3. Set to -1, float("inf") or None to return all items.
            retrieve_nacelle: Whether to retrieve the nacelle
                for each wind turbine or not.
            retrieve_rotor: Whether to retrieve the rotor
                for each wind turbine or not.

        Returns:
            MetmastQueryAPI: The query API for the metmast.
        """
        from .metmast_query import MetmastQueryAPI

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
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                view_id=Distance._view_id,
                max_retrieve_limit=limit,
                connection_property=ViewPropertyId(self._view_id, "metmast"),
            )
        )

        view_id = MetmastQueryAPI._view_id
        has_data = dm.filters.HasData(views=[view_id])
        node_filter = _create_metmast_filter(
            view_id,
            min_position,
            max_position,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        if retrieve_nacelle:
            self._query_append_nacelle(from_)
        if retrieve_rotor:
            self._query_append_rotor(from_)
        return MetmastQueryAPI(
            self._client,
            self._builder,
            self._result_cls,
            self._result_list_cls,
            ViewPropertyId(self._view_id, "end_node"),
            node_filter,
            limit,
        )

    def query(
        self,
        retrieve_nacelle: bool = False,
        retrieve_rotor: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_nacelle: Whether to retrieve the
                nacelle for each
                wind turbine or not.
            retrieve_rotor: Whether to retrieve the
                rotor for each
                wind turbine or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_nacelle:
            self._query_append_nacelle(from_)
        if retrieve_rotor:
            self._query_append_rotor(from_)
        return self._query()

    def _query_append_nacelle(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("nacelle"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[Nacelle._view_id]),
                ),
                view_id=Nacelle._view_id,
                connection_property=ViewPropertyId(self._view_id, "nacelle"),
            ),
        )

    def _query_append_rotor(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("rotor"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[Rotor._view_id]),
                ),
                view_id=Rotor._view_id,
                connection_property=ViewPropertyId(self._view_id, "rotor"),
            ),
        )
