from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

from cognite.client import data_modeling as dm, CogniteClient

from wind_turbine.data_classes import (
    DomainModelCore,
    HighSpeedShaft,
    SensorTimeSeries,
    SensorTimeSeries,
    SensorTimeSeries,
)
from wind_turbine.data_classes._core import (
    DEFAULT_QUERY_LIMIT,
    ViewPropertyId,
    T_DomainModel,
    T_DomainModelList,
    QueryBuilder,
    QueryStep,
)
from wind_turbine._api._core import (
    QueryAPI,
    _create_edge_filter,
)


class HighSpeedShaftQueryAPI(QueryAPI[T_DomainModel, T_DomainModelList]):
    _view_id = dm.ViewId("sp_pygen_power", "HighSpeedShaft", "1")

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

    def query(
        self,
        retrieve_bending_moment_y: bool = False,
        retrieve_bending_monent_x: bool = False,
        retrieve_torque: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_bending_moment_y: Whether to retrieve the
                bending moment y for each
                high speed shaft or not.
            retrieve_bending_monent_x: Whether to retrieve the
                bending monent x for each
                high speed shaft or not.
            retrieve_torque: Whether to retrieve the
                torque for each
                high speed shaft or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_bending_moment_y:
            self._query_append_bending_moment_y(from_)
        if retrieve_bending_monent_x:
            self._query_append_bending_monent_x(from_)
        if retrieve_torque:
            self._query_append_torque(from_)
        return self._query()

    def _query_append_bending_moment_y(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("bending_moment_y"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                view_id=SensorTimeSeries._view_id,
                connection_property=ViewPropertyId(self._view_id, "bending_moment_y"),
            ),
        )

    def _query_append_bending_monent_x(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("bending_monent_x"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                view_id=SensorTimeSeries._view_id,
                connection_property=ViewPropertyId(self._view_id, "bending_monent_x"),
            ),
        )

    def _query_append_torque(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("torque"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                view_id=SensorTimeSeries._view_id,
                connection_property=ViewPropertyId(self._view_id, "torque"),
            ),
        )
