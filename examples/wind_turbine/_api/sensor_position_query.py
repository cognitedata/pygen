from __future__ import annotations

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from wind_turbine._api._core import (
    QueryAPI,
)
from wind_turbine.data_classes import (
    Blade,
    SensorPosition,
    SensorTimeSeries,
)
from wind_turbine.data_classes._core import (
    DEFAULT_QUERY_LIMIT,
    DataClassQueryBuilder,
    NodeQueryStep,
    T_DomainModelList,
)


class SensorPositionQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("sp_pygen_power", "SensorPosition", "1")

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
                result_cls=SensorPosition,
                max_retrieve_limit=limit,
            )
        )

    def query(
        self,
        retrieve_blade: bool = False,
        retrieve_edgewise_bend_mom_crosstalk_corrected: bool = False,
        retrieve_edgewise_bend_mom_offset: bool = False,
        retrieve_edgewise_bend_mom_offset_crosstalk_corrected: bool = False,
        retrieve_edgewisewise_bend_mom: bool = False,
        retrieve_flapwise_bend_mom: bool = False,
        retrieve_flapwise_bend_mom_crosstalk_corrected: bool = False,
        retrieve_flapwise_bend_mom_offset: bool = False,
        retrieve_flapwise_bend_mom_offset_crosstalk_corrected: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_blade: Whether to retrieve the blade
            for each sensor position or not.
            retrieve_edgewise_bend_mom_crosstalk_corrected: Whether to retrieve the edgewise bend mom crosstalk corrected
            for each sensor position or not.
            retrieve_edgewise_bend_mom_offset: Whether to retrieve the edgewise bend mom offset
            for each sensor position or not.
            retrieve_edgewise_bend_mom_offset_crosstalk_corrected: Whether to retrieve the edgewise bend mom offset crosstalk corrected
            for each sensor position or not.
            retrieve_edgewisewise_bend_mom: Whether to retrieve the edgewisewise bend mom
            for each sensor position or not.
            retrieve_flapwise_bend_mom: Whether to retrieve the flapwise bend mom
            for each sensor position or not.
            retrieve_flapwise_bend_mom_crosstalk_corrected: Whether to retrieve the flapwise bend mom crosstalk corrected
            for each sensor position or not.
            retrieve_flapwise_bend_mom_offset: Whether to retrieve the flapwise bend mom offset
            for each sensor position or not.
            retrieve_flapwise_bend_mom_offset_crosstalk_corrected: Whether to retrieve the flapwise bend mom offset crosstalk corrected
            for each sensor position or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_blade:
            self._query_append_blade(from_)
        if retrieve_edgewise_bend_mom_crosstalk_corrected:
            self._query_append_edgewise_bend_mom_crosstalk_corrected(from_)
        if retrieve_edgewise_bend_mom_offset:
            self._query_append_edgewise_bend_mom_offset(from_)
        if retrieve_edgewise_bend_mom_offset_crosstalk_corrected:
            self._query_append_edgewise_bend_mom_offset_crosstalk_corrected(from_)
        if retrieve_edgewisewise_bend_mom:
            self._query_append_edgewisewise_bend_mom(from_)
        if retrieve_flapwise_bend_mom:
            self._query_append_flapwise_bend_mom(from_)
        if retrieve_flapwise_bend_mom_crosstalk_corrected:
            self._query_append_flapwise_bend_mom_crosstalk_corrected(from_)
        if retrieve_flapwise_bend_mom_offset:
            self._query_append_flapwise_bend_mom_offset(from_)
        if retrieve_flapwise_bend_mom_offset_crosstalk_corrected:
            self._query_append_flapwise_bend_mom_offset_crosstalk_corrected(from_)
        return self._query()

    def _query_append_blade(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("blade"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[Blade._view_id]),
                ),
                result_cls=Blade,
            ),
        )

    def _query_append_edgewise_bend_mom_crosstalk_corrected(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("edgewise_bend_mom_crosstalk_corrected"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                result_cls=SensorTimeSeries,
            ),
        )

    def _query_append_edgewise_bend_mom_offset(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("edgewise_bend_mom_offset"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                result_cls=SensorTimeSeries,
            ),
        )

    def _query_append_edgewise_bend_mom_offset_crosstalk_corrected(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("edgewise_bend_mom_offset_crosstalk_corrected"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                result_cls=SensorTimeSeries,
            ),
        )

    def _query_append_edgewisewise_bend_mom(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("edgewisewise_bend_mom"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                result_cls=SensorTimeSeries,
            ),
        )

    def _query_append_flapwise_bend_mom(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("flapwise_bend_mom"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                result_cls=SensorTimeSeries,
            ),
        )

    def _query_append_flapwise_bend_mom_crosstalk_corrected(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("flapwise_bend_mom_crosstalk_corrected"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                result_cls=SensorTimeSeries,
            ),
        )

    def _query_append_flapwise_bend_mom_offset(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("flapwise_bend_mom_offset"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                result_cls=SensorTimeSeries,
            ),
        )

    def _query_append_flapwise_bend_mom_offset_crosstalk_corrected(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("flapwise_bend_mom_offset_crosstalk_corrected"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[SensorTimeSeries._view_id]),
                ),
                result_cls=SensorTimeSeries,
            ),
        )
