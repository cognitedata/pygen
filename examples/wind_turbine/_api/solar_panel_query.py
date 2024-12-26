from __future__ import annotations

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from wind_turbine._api._core import (
    QueryAPI,
)
from wind_turbine.data_classes import (
    SensorTimeSeries,
    SolarPanel,
)
from wind_turbine.data_classes._core import (
    DEFAULT_QUERY_LIMIT,
    DataClassQueryBuilder,
    NodeQueryStep,
    T_DomainModelList,
)


class SolarPanelQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("sp_pygen_power", "SolarPanel", "1")

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
                result_cls=SolarPanel,
                max_retrieve_limit=limit,
            )
        )

    def query(
        self,
        retrieve_efficiency: bool = False,
        retrieve_orientation: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_efficiency: Whether to retrieve the
                efficiency for each
                solar panel or not.
            retrieve_orientation: Whether to retrieve the
                orientation for each
                solar panel or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_efficiency:
            self._query_append_efficiency(from_)
        if retrieve_orientation:
            self._query_append_orientation(from_)
        return self._query()

    def _query_append_efficiency(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("efficiency"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                result_cls=SensorTimeSeries,
            ),
        )

    def _query_append_orientation(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("orientation"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                result_cls=SensorTimeSeries,
            ),
        )
