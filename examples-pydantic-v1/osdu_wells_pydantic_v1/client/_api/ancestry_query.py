from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList
from osdu_wells_pydantic_v1.client.data_classes import (
    Ancestry,
    AncestryApply,
)
from osdu_wells_pydantic_v1.client.data_classes._ancestry import (
    _ANCESTRY_PROPERTIES_BY_FIELD,
)


class AncestryQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_ancestry: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_ancestry: Whether to retrieve the ancestry or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_ancestry and not self._builder[-1].name.startswith("ancestry"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("ancestry"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[AncestryApply],
                                list(_ANCESTRY_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=Ancestry,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
