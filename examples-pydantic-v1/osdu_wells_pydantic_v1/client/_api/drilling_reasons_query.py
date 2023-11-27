from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList
from osdu_wells_pydantic_v1.client.data_classes import (
    DrillingReasons,
    DrillingReasonsApply,
)
from osdu_wells_pydantic_v1.client.data_classes._drilling_reasons import (
    _DRILLINGREASONS_PROPERTIES_BY_FIELD,
)


class DrillingReasonsQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_drilling_reason: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_drilling_reason: Whether to retrieve the drilling reason or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_drilling_reason and not self._builder[-1].name.startswith("drilling_reason"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("drilling_reason"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[DrillingReasonsApply],
                                list(_DRILLINGREASONS_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=DrillingReasons,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
