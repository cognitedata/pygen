from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList
from movie_domain.client.data_classes import (
    Rating,
    RatingApply,
)
from movie_domain.client.data_classes._rating import (
    _RATING_PROPERTIES_BY_FIELD,
)


class RatingQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_rating: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_rating: Whether to retrieve the rating or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_rating and not self._builder[-1].name.startswith("rating"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("rating"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[RatingApply],
                                list(_RATING_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=Rating,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
