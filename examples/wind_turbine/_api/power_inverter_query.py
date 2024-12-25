from __future__ import annotations

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from wind_turbine._api._core import (
    QueryAPI,
)
from wind_turbine.data_classes import (
    PowerInverter,
    SensorTimeSeries,
)
from wind_turbine.data_classes._core import (
    DEFAULT_QUERY_LIMIT,
    DataClassQueryBuilder,
    NodeQueryStep,
    T_DomainModelList,
)


class PowerInverterQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("sp_pygen_power", "PowerInverter", "1")

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
                result_cls=PowerInverter,
                max_retrieve_limit=limit,
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
            retrieve_active_power_total: Whether to retrieve the active power total for each power inverter or not.
            retrieve_apparent_power_total: Whether to retrieve the apparent power total for each power inverter or not.
            retrieve_reactive_power_total: Whether to retrieve the reactive power total for each power inverter or not.

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
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("active_power_total"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                result_cls=SensorTimeSeries,
            ),
        )

    def _query_append_apparent_power_total(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("apparent_power_total"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                result_cls=SensorTimeSeries,
            ),
        )

    def _query_append_reactive_power_total(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("reactive_power_total"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                result_cls=SensorTimeSeries,
            ),
        )
