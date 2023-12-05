from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from osdu_wells.client.data_classes import (
    DomainModelApply,
    TechnicalAssurances,
    TechnicalAssurancesApply,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .acceptable_usage_query import AcceptableUsageQueryAPI
    from .reviewers_query import ReviewersQueryAPI
    from .unacceptable_usage_query import UnacceptableUsageQueryAPI


class TechnicalAssurancesQueryAPI(QueryAPI[T_DomainModelList]):
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
                name=self._builder.next_name("technical_assurance"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select(
                    [dm.query.SourceSelector(self._view_by_write_class[TechnicalAssurancesApply], ["*"])]
                ),
                result_cls=TechnicalAssurances,
                max_retrieve_limit=limit,
            )
        )

    def acceptable_usage(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
    ) -> AcceptableUsageQueryAPI[T_DomainModelList]:
        """Query along the acceptable usage edges of the technical assurance.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of acceptable usage edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            AcceptableUsageQueryAPI: The query API for the acceptable usage.
        """
        from .acceptable_usage_query import AcceptableUsageQueryAPI

        from_ = self._builder[-1].name

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "TechnicalAssurances.AcceptableUsage"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("acceptable_usage"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        return AcceptableUsageQueryAPI(self._client, self._builder, self._view_by_write_class, None, limit)

    def reviewers(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
    ) -> ReviewersQueryAPI[T_DomainModelList]:
        """Query along the reviewer edges of the technical assurance.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of reviewer edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            ReviewersQueryAPI: The query API for the reviewer.
        """
        from .reviewers_query import ReviewersQueryAPI

        from_ = self._builder[-1].name

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "TechnicalAssurances.Reviewers"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("reviewers"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        return ReviewersQueryAPI(self._client, self._builder, self._view_by_write_class, None, limit)

    def unacceptable_usage(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
    ) -> UnacceptableUsageQueryAPI[T_DomainModelList]:
        """Query along the unacceptable usage edges of the technical assurance.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of unacceptable usage edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            UnacceptableUsageQueryAPI: The query API for the unacceptable usage.
        """
        from .unacceptable_usage_query import UnacceptableUsageQueryAPI

        from_ = self._builder[-1].name

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("IntegrationTestsImmutable", "TechnicalAssurances.UnacceptableUsage"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("unacceptable_usage"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        return UnacceptableUsageQueryAPI(self._client, self._builder, self._view_by_write_class, None, limit)

    def query(
        self,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Returns:
            The list of the source nodes of the query.

        """
        return self._query()
