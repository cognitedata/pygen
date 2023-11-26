from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter
from osdu_wells.client.data_classes import (
    TechnicalAssurances,
    TechnicalAssurancesApply,
    AcceptableUsage,
    AcceptableUsageApply,
    Reviewers,
    ReviewersApply,
    UnacceptableUsage,
    UnacceptableUsageApply,
)
from osdu_wells.client.data_classes._technical_assurances import (
    _TECHNICALASSURANCES_PROPERTIES_BY_FIELD,
)
from osdu_wells.client.data_classes._acceptable_usage import (
    _ACCEPTABLEUSAGE_PROPERTIES_BY_FIELD,
)
from osdu_wells.client.data_classes._reviewers import (
    _REVIEWERS_PROPERTIES_BY_FIELD,
)
from osdu_wells.client.data_classes._unacceptable_usage import (
    _UNACCEPTABLEUSAGE_PROPERTIES_BY_FIELD,
)

if TYPE_CHECKING:
    from .acceptable_usage_query import AcceptableUsageQueryAPI
    from .reviewers_query import ReviewersQueryAPI
    from .unacceptable_usage_query import UnacceptableUsageQueryAPI


class TechnicalAssurancesQueryAPI(QueryAPI[T_DomainModelList]):
    def acceptable_usage(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = None,
    ) -> AcceptableUsageQueryAPI[T_DomainModelList]:
        """Query along the acceptable usage edges of the technical assurance.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            AcceptableUsageQueryAPI: The query API for the acceptable usage.
        """
        from .acceptable_usage_query import AcceptableUsageQueryAPI

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
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("acceptable_usage"),
                expression=dm.query.NodeResultSetExpression(
                    filter=None,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(
                    [
                        dm.query.SourceSelector(
                            self._view_by_write_class[AcceptableUsageApply],
                            list(_ACCEPTABLEUSAGE_PROPERTIES_BY_FIELD.values()),
                        )
                    ]
                ),
                result_cls=AcceptableUsage,
                max_retrieve_limit=-1,
            ),
        )
        return AcceptableUsageQueryAPI(self._client, self._builder, self._view_by_write_class)

    def reviewers(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = None,
    ) -> ReviewersQueryAPI[T_DomainModelList]:
        """Query along the reviewer edges of the technical assurance.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            ReviewersQueryAPI: The query API for the reviewer.
        """
        from .reviewers_query import ReviewersQueryAPI

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
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("reviewer"),
                expression=dm.query.NodeResultSetExpression(
                    filter=None,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(
                    [
                        dm.query.SourceSelector(
                            self._view_by_write_class[ReviewersApply],
                            list(_REVIEWERS_PROPERTIES_BY_FIELD.values()),
                        )
                    ]
                ),
                result_cls=Reviewers,
                max_retrieve_limit=-1,
            ),
        )
        return ReviewersQueryAPI(self._client, self._builder, self._view_by_write_class)

    def unacceptable_usage(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = None,
    ) -> UnacceptableUsageQueryAPI[T_DomainModelList]:
        """Query along the unacceptable usage edges of the technical assurance.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of work unit edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            UnacceptableUsageQueryAPI: The query API for the unacceptable usage.
        """
        from .unacceptable_usage_query import UnacceptableUsageQueryAPI

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
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("unacceptable_usage"),
                expression=dm.query.NodeResultSetExpression(
                    filter=None,
                    from_=self._builder[-1].name,
                ),
                select=dm.query.Select(
                    [
                        dm.query.SourceSelector(
                            self._view_by_write_class[UnacceptableUsageApply],
                            list(_UNACCEPTABLEUSAGE_PROPERTIES_BY_FIELD.values()),
                        )
                    ]
                ),
                result_cls=UnacceptableUsage,
                max_retrieve_limit=-1,
            ),
        )
        return UnacceptableUsageQueryAPI(self._client, self._builder, self._view_by_write_class)

    def query(
        self,
        retrieve_technical_assurance: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_technical_assurance: Whether to retrieve the technical assurance or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_technical_assurance and not self._builder[-1].name.startswith("technical_assurance"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("technical_assurance"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[TechnicalAssurancesApply],
                                list(_TECHNICALASSURANCES_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=TechnicalAssurances,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
