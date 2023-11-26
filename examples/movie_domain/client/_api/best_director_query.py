from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import QueryStep, QueryAPI, T_DomainModelList
from movie_domain.client.data_classes import (
    BestDirector,
    BestDirectorApply,
)
from movie_domain.client.data_classes._best_director import (
    _BESTDIRECTOR_PROPERTIES_BY_FIELD,
)


class BestDirectorQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_best_director: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_best_director: Whether to retrieve the best director or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_best_director and not self._builder[-1].name.startswith("best_director"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("best_director"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[BestDirectorApply],
                                list(_BESTDIRECTOR_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=BestDirector,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
