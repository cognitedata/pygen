from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

from cognite.client import data_modeling as dm, CogniteClient

from wind_turbine.data_classes import (
    DomainModelCore,
    PowerInverter,
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


class PowerInverterQueryAPI(QueryAPI[T_DomainModel, T_DomainModelList]):
    _view_id = dm.ViewId("sp_pygen_power", "PowerInverter", "1")

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
        retrieve_active_power_total: bool = False,
        retrieve_apparent_power_total: bool = False,
        retrieve_reactive_power_total: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_active_power_total: Whether to retrieve the
                active power total for each
                power inverter or not.
            retrieve_apparent_power_total: Whether to retrieve the
                apparent power total for each
                power inverter or not.
            retrieve_reactive_power_total: Whether to retrieve the
                reactive power total for each
                power inverter or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_active_power_total:
            self._query_append_active_power_total(from_)
        if retrieve_apparent_power_total:
            self._query_append_apparent_power_total(from_)
        if retrieve_reactive_power_total:
            self._query_append_reactive_power_total(from_)
        return self._query()

    def _query_append_active_power_total(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("active_power_total"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                view_id=SensorTimeSeries._view_id,
                connection_property=ViewPropertyId(self._view_id, "active_power_total"),
            ),
        )

    def _query_append_apparent_power_total(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("apparent_power_total"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                view_id=SensorTimeSeries._view_id,
                connection_property=ViewPropertyId(self._view_id, "apparent_power_total"),
            ),
        )

    def _query_append_reactive_power_total(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("reactive_power_total"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                view_id=SensorTimeSeries._view_id,
                connection_property=ViewPropertyId(self._view_id, "reactive_power_total"),
            ),
        )
