from __future__ import annotations

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from wind_turbine._api._core import (
    QueryAPI,
)
from wind_turbine.data_classes import (
    Rotor,
    SensorTimeSeries,
)
from wind_turbine.data_classes._core import (
    DEFAULT_QUERY_LIMIT,
    DataClassQueryBuilder,
    NodeQueryStep,
    T_DomainModelList,
)


class RotorQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("sp_pygen_power", "Rotor", "1")

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
                result_cls=Rotor,
                max_retrieve_limit=limit,
            )
        )

    def query(
        self,
        retrieve_rotor_speed_controller: bool = False,
        retrieve_rpm_low_speed_shaft: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_rotor_speed_controller: Whether to retrieve the
                rotor speed controller for each
                rotor or not.
            retrieve_rpm_low_speed_shaft: Whether to retrieve the
                rpm low speed shaft for each
                rotor or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_rotor_speed_controller:
            self._query_append_rotor_speed_controller(from_)
        if retrieve_rpm_low_speed_shaft:
            self._query_append_rpm_low_speed_shaft(from_)
        return self._query()

    def _query_append_rotor_speed_controller(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("rotor_speed_controller"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                result_cls=SensorTimeSeries,
            ),
        )

    def _query_append_rpm_low_speed_shaft(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("rpm_low_speed_shaft"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                result_cls=SensorTimeSeries,
            ),
        )
