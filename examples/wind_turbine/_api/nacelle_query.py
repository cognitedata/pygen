from __future__ import annotations

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from wind_turbine._api._core import (
    QueryAPI,
)
from wind_turbine.data_classes import (
    Gearbox,
    Generator,
    HighSpeedShaft,
    MainShaft,
    Nacelle,
    PowerInverter,
    SensorTimeSeries,
)
from wind_turbine.data_classes._core import (
    DEFAULT_QUERY_LIMIT,
    DataClassQueryBuilder,
    NodeQueryStep,
    T_DomainModelList,
)


class NacelleQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("sp_pygen_power", "Nacelle", "1")

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
                result_cls=Nacelle,
                max_retrieve_limit=limit,
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
            retrieve_acc_from_back_side_y: Whether to retrieve the acc from back side y for each nacelle or not.
            retrieve_acc_from_back_side_z: Whether to retrieve the acc from back side z for each nacelle or not.
            retrieve_gearbox: Whether to retrieve the gearbox for each nacelle or not.
            retrieve_generator: Whether to retrieve the generator for each nacelle or not.
            retrieve_high_speed_shaft: Whether to retrieve the high speed shaft for each nacelle or not.
            retrieve_main_shaft: Whether to retrieve the main shaft for each nacelle or not.
            retrieve_power_inverter: Whether to retrieve the power inverter for each nacelle or not.
            retrieve_yaw_direction: Whether to retrieve the yaw direction for each nacelle or not.
            retrieve_yaw_error: Whether to retrieve the yaw error for each nacelle or not.

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
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("acc_from_back_side_y"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                result_cls=SensorTimeSeries,
            ),
        )

    def _query_append_acc_from_back_side_z(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("acc_from_back_side_z"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                result_cls=SensorTimeSeries,
            ),
        )

    def _query_append_gearbox(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("gearbox"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[Gearbox._view_id]),
                ),
                result_cls=Gearbox,
            ),
        )

    def _query_append_generator(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("generator"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[Generator._view_id]),
                ),
                result_cls=Generator,
            ),
        )

    def _query_append_high_speed_shaft(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("high_speed_shaft"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[HighSpeedShaft._view_id]),
                ),
                result_cls=HighSpeedShaft,
            ),
        )

    def _query_append_main_shaft(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("main_shaft"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[MainShaft._view_id]),
                ),
                result_cls=MainShaft,
            ),
        )

    def _query_append_power_inverter(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("power_inverter"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[PowerInverter._view_id]),
                ),
                result_cls=PowerInverter,
            ),
        )

    def _query_append_yaw_direction(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("yaw_direction"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                result_cls=SensorTimeSeries,
            ),
        )

    def _query_append_yaw_error(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("yaw_error"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                result_cls=SensorTimeSeries,
            ),
        )
