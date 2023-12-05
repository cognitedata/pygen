from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from osdu_wells.client.data_classes import (
    DomainModelApply,
    WgsCoordinates,
    WgsCoordinatesApply,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .features_query import FeaturesQueryAPI


class WgsCoordinatesQueryAPI(QueryAPI[T_DomainModelList]):
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
                name=self._builder.next_name("wgs_84_coordinate"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select(
                    [dm.query.SourceSelector(self._view_by_write_class[WgsCoordinatesApply], ["*"])]
                ),
                result_cls=WgsCoordinates,
                max_retrieve_limit=limit,
            )
        )

    def features(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
    ) -> FeaturesQueryAPI[T_DomainModelList]:
        """Query along the feature edges of the wgs 84 coordinate.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of feature edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            FeaturesQueryAPI: The query API for the feature.
        """
        from .features_query import FeaturesQueryAPI

        from_ = self._builder[-1].name

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "Wgs84Coordinates.features"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("features"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        return FeaturesQueryAPI(self._client, self._builder, self._view_by_write_class, None, limit)

    def query(
        self,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Returns:
            The list of the source nodes of the query.

        """
        return self._query()
