from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from markets.client.data_classes import (
    DomainModelApply,
    DateTransformationPair,
    DateTransformationPairApply,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .date_transformation_query import DateTransformationQueryAPI
    from .date_transformation_query import DateTransformationQueryAPI


class DateTransformationPairQueryAPI(QueryAPI[T_DomainModelList]):
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
                name=self._builder.next_name("date_transformation_pair"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select(
                    [dm.query.SourceSelector(self._view_by_write_class[DateTransformationPairApply], ["*"])]
                ),
                result_cls=DateTransformationPair,
                max_retrieve_limit=limit,
            )
        )

    def end(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
    ) -> DateTransformationQueryAPI[T_DomainModelList]:
        """Query along the end edges of the date transformation pair.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of end edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            DateTransformationQueryAPI: The query API for the date transformation.
        """
        from .date_transformation_query import DateTransformationQueryAPI

        from_ = self._builder[-1].name

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("market", "DateTransformationPair.end"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("end"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        return DateTransformationQueryAPI(self._client, self._builder, self._view_by_write_class, None, limit)

    def start(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
    ) -> DateTransformationQueryAPI[T_DomainModelList]:
        """Query along the start edges of the date transformation pair.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of start edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            DateTransformationQueryAPI: The query API for the date transformation.
        """
        from .date_transformation_query import DateTransformationQueryAPI

        from_ = self._builder[-1].name

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("market", "DateTransformationPair.start"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("start"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        return DateTransformationQueryAPI(self._client, self._builder, self._view_by_write_class, None, limit)

    def query(
        self,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Returns:
            The list of the source nodes of the query.

        """
        return self._query()
