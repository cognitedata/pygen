from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList
from osdu_wells_pydantic_v1.client.data_classes import (
    Legal,
    LegalApply,
)
from osdu_wells_pydantic_v1.client.data_classes._legal import (
    _LEGAL_PROPERTIES_BY_FIELD,
)


class LegalQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_legal: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_legal: Whether to retrieve the legal or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_legal and not self._builder[-1].name.startswith("legal"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("legal"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[LegalApply],
                                list(_LEGAL_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=Legal,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()