from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import QueryStep, QueryAPI, T_DomainModelList
from movie_domain.client.data_classes import (
    BestLeadingActor,
    BestLeadingActorApply,
)
from movie_domain.client.data_classes._best_leading_actor import (
    _BESTLEADINGACTOR_PROPERTIES_BY_FIELD,
)


class BestLeadingActorQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_best_leading_actor: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_best_leading_actor: Whether to retrieve the best leading actor or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_best_leading_actor and not self._builder[-1].name.startswith("best_leading_actor"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("best_leading_actor"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[BestLeadingActorApply],
                                list(_BESTLEADINGACTOR_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=BestLeadingActor,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
