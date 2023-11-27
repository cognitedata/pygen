from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList
from movie_domain.client.data_classes import (
    BestLeadingActress,
    BestLeadingActressApply,
)
from movie_domain.client.data_classes._best_leading_actress import (
    _BESTLEADINGACTRESS_PROPERTIES_BY_FIELD,
)


class BestLeadingActressQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_best_leading_actress: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_best_leading_actress: Whether to retrieve the best leading actress or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_best_leading_actress and not self._builder[-1].name.startswith("best_leading_actress"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("best_leading_actress"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[BestLeadingActressApply],
                                list(_BESTLEADINGACTRESS_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=BestLeadingActress,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
