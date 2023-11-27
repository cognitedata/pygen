from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList
from equipment_unit.client.data_classes import (
    StartEndTime,
    StartEndTimeApply,
)
from equipment_unit.client.data_classes._start_end_time import (
    _STARTENDTIME_PROPERTIES_BY_FIELD,
)


class StartEndTimeQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_start_end_time: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_start_end_time: Whether to retrieve the start end time or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_start_end_time and not self._builder[-1].name.startswith("start_end_time"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("start_end_time"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[StartEndTimeApply],
                                list(_STARTENDTIME_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=StartEndTime,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
