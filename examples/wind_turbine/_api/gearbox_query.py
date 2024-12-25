from __future__ import annotations

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from wind_turbine._api._core import (
    QueryAPI,
)
from wind_turbine.data_classes import (
    Gearbox,
    SensorTimeSeries,
)
from wind_turbine.data_classes._core import (
    DEFAULT_QUERY_LIMIT,
    DataClassQueryBuilder,
    NodeQueryStep,
    T_DomainModelList,
)


class GearboxQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("sp_pygen_power", "Gearbox", "1")

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
                result_cls=Gearbox,
                max_retrieve_limit=limit,
            )
        )

    def query(
        self,
        retrieve_displacement_x: bool = False,
        retrieve_displacement_y: bool = False,
        retrieve_displacement_z: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_displacement_x: Whether to retrieve the displacement x for each gearbox or not.
            retrieve_displacement_y: Whether to retrieve the displacement y for each gearbox or not.
            retrieve_displacement_z: Whether to retrieve the displacement z for each gearbox or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_displacement_x:
            self._query_append_displacement_x(from_)
        if retrieve_displacement_y:
            self._query_append_displacement_y(from_)
        if retrieve_displacement_z:
            self._query_append_displacement_z(from_)
        return self._query()

    def _query_append_displacement_x(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("displacement_x"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                result_cls=SensorTimeSeries,
            ),
        )

    def _query_append_displacement_y(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("displacement_y"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                result_cls=SensorTimeSeries,
            ),
        )

    def _query_append_displacement_z(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("displacement_z"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                result_cls=SensorTimeSeries,
            ),
        )
