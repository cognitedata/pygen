from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import QueryStep, QueryAPI, T_DomainModelList
from movie_domain.client.data_classes import (
    Nomination,
    NominationApply,
)
from movie_domain.client.data_classes._nomination import (
    _NOMINATION_PROPERTIES_BY_FIELD,
)


class NominationQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_nomination: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_nomination: Whether to retrieve the nomination or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_nomination and not self._builder[-1].name.startswith("nomination"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("nomination"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[NominationApply],
                                list(_NOMINATION_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=Nomination,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
