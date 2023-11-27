from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList
from osdu_wells_pydantic_v1.client.data_classes import (
    HistoricalInterests,
    HistoricalInterestsApply,
)
from osdu_wells_pydantic_v1.client.data_classes._historical_interests import (
    _HISTORICALINTERESTS_PROPERTIES_BY_FIELD,
)


class HistoricalInterestsQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_historical_interest: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_historical_interest: Whether to retrieve the historical interest or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_historical_interest and not self._builder[-1].name.startswith("historical_interest"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("historical_interest"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[HistoricalInterestsApply],
                                list(_HISTORICALINTERESTS_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=HistoricalInterests,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
