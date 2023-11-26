from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import QueryStep, QueryAPI, T_DomainModelList
from osdu_wells.client.data_classes import (
    UnacceptableUsage,
    UnacceptableUsageApply,
)
from osdu_wells.client.data_classes._unacceptable_usage import (
    _UNACCEPTABLEUSAGE_PROPERTIES_BY_FIELD,
)


class UnacceptableUsageQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_unacceptable_usage: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_unacceptable_usage: Whether to retrieve the unacceptable usage or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_unacceptable_usage and not self._builder[-1].name.startswith("unacceptable_usage"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("unacceptable_usage"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[UnacceptableUsageApply],
                                list(_UNACCEPTABLEUSAGE_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=UnacceptableUsage,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
