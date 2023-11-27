from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList
from osdu_wells.client.data_classes import (
    Reviewers,
    ReviewersApply,
)
from osdu_wells.client.data_classes._reviewers import (
    _REVIEWERS_PROPERTIES_BY_FIELD,
)


class ReviewersQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_reviewer: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_reviewer: Whether to retrieve the reviewer or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_reviewer and not self._builder[-1].name.startswith("reviewer"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("reviewer"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
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

        return self._query()
