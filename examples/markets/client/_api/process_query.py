from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import QueryStep, QueryAPI, T_DomainModelList
from markets.client.data_classes import (
    Process,
    ProcessApply,
)
from markets.client.data_classes._process import (
    _PROCESS_PROPERTIES_BY_FIELD,
)


class ProcessQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_proces: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_proces: Whether to retrieve the proces or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_proces and not self._builder[-1].name.startswith("proces"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("proces"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[ProcessApply],
                                list(_PROCESS_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=Process,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
