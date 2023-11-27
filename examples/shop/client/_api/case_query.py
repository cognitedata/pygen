from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList
from shop.client.data_classes import (
    Case,
    CaseApply,
)
from shop.client.data_classes._case import (
    _CASE_PROPERTIES_BY_FIELD,
)


class CaseQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_case: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_case: Whether to retrieve the case or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_case and not self._builder[-1].name.startswith("case"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("case"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[CaseApply],
                                list(_CASE_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=Case,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
