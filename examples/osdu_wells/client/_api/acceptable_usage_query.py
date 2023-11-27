from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList
from osdu_wells.client.data_classes import (
    AcceptableUsage,
    AcceptableUsageApply,
)
from osdu_wells.client.data_classes._acceptable_usage import (
    _ACCEPTABLEUSAGE_PROPERTIES_BY_FIELD,
)


class AcceptableUsageQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_acceptable_usage: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_acceptable_usage: Whether to retrieve the acceptable usage or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_acceptable_usage and not self._builder[-1].name.startswith("acceptable_usage"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("acceptable_usage"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[AcceptableUsageApply],
                                list(_ACCEPTABLEUSAGE_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=AcceptableUsage,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
