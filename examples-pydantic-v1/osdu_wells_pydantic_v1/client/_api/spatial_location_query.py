from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from osdu_wells_pydantic_v1.client.data_classes import (
    DomainModelApply,
    SpatialLocation,
    SpatialLocationApply,
    AsIngestedCoordinates,
    AsIngestedCoordinatesApply,
    WgsCoordinates,
    WgsCoordinatesApply,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter


class SpatialLocationQueryAPI(QueryAPI[T_DomainModelList]):
    def __init__(
        self,
        client: CogniteClient,
        builder: QueryBuilder[T_DomainModelList],
        view_by_write_class: dict[type[DomainModelApply], dm.ViewId],
        filter_: dm.filters.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ):
        super().__init__(client, builder, view_by_write_class)

        self._builder.append(
            QueryStep(
                name=self._builder.next_name("spatial_location"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select(
                    [dm.query.SourceSelector(self._view_by_write_class[SpatialLocationApply], ["*"])]
                ),
                result_cls=SpatialLocation,
                max_retrieve_limit=limit,
            )
        )

    def query(
        self,
        retrieve_as_ingested_coordinates: bool = False,
        retrieve_wgs_84_coordinates: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_as_ingested_coordinates: Whether to retrieve the as ingested coordinate for each spatial location or not.
            retrieve_wgs_84_coordinates: Whether to retrieve the wgs 84 coordinate for each spatial location or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_as_ingested_coordinates:
            self._query_append_as_ingested_coordinates(from_)
        if retrieve_wgs_84_coordinates:
            self._query_append_wgs_84_coordinates(from_)
        return self._query()

    def _query_append_as_ingested_coordinates(self, from_: str) -> None:
        view_id = self._view_by_write_class[AsIngestedCoordinatesApply]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("as_ingested_coordinates"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_write_class[SpatialLocationApply].as_property_ref("as_ingested_coordinates"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=AsIngestedCoordinates,
            ),
        )

    def _query_append_wgs_84_coordinates(self, from_: str) -> None:
        view_id = self._view_by_write_class[WgsCoordinatesApply]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("wgs_84_coordinates"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_write_class[SpatialLocationApply].as_property_ref("wgs_84_coordinates"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=WgsCoordinates,
            ),
        )
