from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

from cognite.client import data_modeling as dm, CogniteClient

from wind_turbine.data_classes import (
    DomainModelCore,
    Nacelle,
    SensorTimeSeries,
    SensorTimeSeries,
    Gearbox,
    Generator,
    HighSpeedShaft,
    MainShaft,
    PowerInverter,
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


class NacelleQueryAPI(QueryAPI[T_DomainModel, T_DomainModelList]):
    _view_id = dm.ViewId("sp_pygen_power", "Nacelle", "1")

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
        retrieve_acc_from_back_side_y: bool = False,
        retrieve_acc_from_back_side_z: bool = False,
        retrieve_gearbox: bool = False,
        retrieve_generator: bool = False,
        retrieve_high_speed_shaft: bool = False,
        retrieve_main_shaft: bool = False,
        retrieve_power_inverter: bool = False,
        retrieve_yaw_direction: bool = False,
        retrieve_yaw_error: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_acc_from_back_side_y: Whether to retrieve the
                acc from back side y for each
                nacelle or not.
            retrieve_acc_from_back_side_z: Whether to retrieve the
                acc from back side z for each
                nacelle or not.
            retrieve_gearbox: Whether to retrieve the
                gearbox for each
                nacelle or not.
            retrieve_generator: Whether to retrieve the
                generator for each
                nacelle or not.
            retrieve_high_speed_shaft: Whether to retrieve the
                high speed shaft for each
                nacelle or not.
            retrieve_main_shaft: Whether to retrieve the
                main shaft for each
                nacelle or not.
            retrieve_power_inverter: Whether to retrieve the
                power inverter for each
                nacelle or not.
            retrieve_yaw_direction: Whether to retrieve the
                yaw direction for each
                nacelle or not.
            retrieve_yaw_error: Whether to retrieve the
                yaw error for each
                nacelle or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_acc_from_back_side_y:
            self._query_append_acc_from_back_side_y(from_)
        if retrieve_acc_from_back_side_z:
            self._query_append_acc_from_back_side_z(from_)
        if retrieve_gearbox:
            self._query_append_gearbox(from_)
        if retrieve_generator:
            self._query_append_generator(from_)
        if retrieve_high_speed_shaft:
            self._query_append_high_speed_shaft(from_)
        if retrieve_main_shaft:
            self._query_append_main_shaft(from_)
        if retrieve_power_inverter:
            self._query_append_power_inverter(from_)
        if retrieve_yaw_direction:
            self._query_append_yaw_direction(from_)
        if retrieve_yaw_error:
            self._query_append_yaw_error(from_)
        return self._query()

    def _query_append_acc_from_back_side_y(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("acc_from_back_side_y"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                view_id=SensorTimeSeries._view_id,
                connection_property=ViewPropertyId(self._view_id, "acc_from_back_side_y"),
            ),
        )

    def _query_append_acc_from_back_side_z(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("acc_from_back_side_z"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                view_id=SensorTimeSeries._view_id,
                connection_property=ViewPropertyId(self._view_id, "acc_from_back_side_z"),
            ),
        )

    def _query_append_gearbox(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("gearbox"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[Gearbox._view_id]),
                ),
                view_id=Gearbox._view_id,
                connection_property=ViewPropertyId(self._view_id, "gearbox"),
            ),
        )

    def _query_append_generator(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("generator"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[Generator._view_id]),
                ),
                view_id=Generator._view_id,
                connection_property=ViewPropertyId(self._view_id, "generator"),
            ),
        )

    def _query_append_high_speed_shaft(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("high_speed_shaft"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[HighSpeedShaft._view_id]),
                ),
                view_id=HighSpeedShaft._view_id,
                connection_property=ViewPropertyId(self._view_id, "high_speed_shaft"),
            ),
        )

    def _query_append_main_shaft(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("main_shaft"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[MainShaft._view_id]),
                ),
                view_id=MainShaft._view_id,
                connection_property=ViewPropertyId(self._view_id, "main_shaft"),
            ),
        )

    def _query_append_power_inverter(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("power_inverter"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[PowerInverter._view_id]),
                ),
                view_id=PowerInverter._view_id,
                connection_property=ViewPropertyId(self._view_id, "power_inverter"),
            ),
        )

    def _query_append_yaw_direction(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("yaw_direction"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                view_id=SensorTimeSeries._view_id,
                connection_property=ViewPropertyId(self._view_id, "yaw_direction"),
            ),
        )

    def _query_append_yaw_error(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("yaw_error"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                view_id=SensorTimeSeries._view_id,
                connection_property=ViewPropertyId(self._view_id, "yaw_error"),
            ),
        )
