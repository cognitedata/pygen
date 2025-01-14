from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

from cognite.client import data_modeling as dm, CogniteClient

from wind_turbine.data_classes import (
    DomainModelCore,
    MainShaft,
    SensorTimeSeries,
    SensorTimeSeries,
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


class MainShaftQueryAPI(QueryAPI[T_DomainModel, T_DomainModelList]):
    _view_id = dm.ViewId("sp_pygen_power", "MainShaft", "1")

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
        retrieve_bending_x: bool = False,
        retrieve_bending_y: bool = False,
        retrieve_calculated_tilt_moment: bool = False,
        retrieve_calculated_yaw_moment: bool = False,
        retrieve_torque: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_bending_x: Whether to retrieve the
                bending x for each
                main shaft or not.
            retrieve_bending_y: Whether to retrieve the
                bending y for each
                main shaft or not.
            retrieve_calculated_tilt_moment: Whether to retrieve the
                calculated tilt moment for each
                main shaft or not.
            retrieve_calculated_yaw_moment: Whether to retrieve the
                calculated yaw moment for each
                main shaft or not.
            retrieve_torque: Whether to retrieve the
                torque for each
                main shaft or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_bending_x:
            self._query_append_bending_x(from_)
        if retrieve_bending_y:
            self._query_append_bending_y(from_)
        if retrieve_calculated_tilt_moment:
            self._query_append_calculated_tilt_moment(from_)
        if retrieve_calculated_yaw_moment:
            self._query_append_calculated_yaw_moment(from_)
        if retrieve_torque:
            self._query_append_torque(from_)
        return self._query()

    def _query_append_bending_x(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("bending_x"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                view_id=SensorTimeSeries._view_id,
                connection_property=ViewPropertyId(self._view_id, "bending_x"),
            ),
        )

    def _query_append_bending_y(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("bending_y"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                view_id=SensorTimeSeries._view_id,
                connection_property=ViewPropertyId(self._view_id, "bending_y"),
            ),
        )

    def _query_append_calculated_tilt_moment(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("calculated_tilt_moment"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                view_id=SensorTimeSeries._view_id,
                connection_property=ViewPropertyId(self._view_id, "calculated_tilt_moment"),
            ),
        )

    def _query_append_calculated_yaw_moment(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("calculated_yaw_moment"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                view_id=SensorTimeSeries._view_id,
                connection_property=ViewPropertyId(self._view_id, "calculated_yaw_moment"),
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
