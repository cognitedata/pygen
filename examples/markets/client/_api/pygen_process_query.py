from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import QueryStep, QueryAPI, T_DomainModelList
from markets.client.data_classes import (
    PygenProcess,
    PygenProcessApply,
)
from markets.client.data_classes._pygen_process import (
    _PYGENPROCESS_PROPERTIES_BY_FIELD,
)


class PygenProcessQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_pygen_proces: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_pygen_proces: Whether to retrieve the pygen proces or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_pygen_proces and not self._builder[-1].name.startswith("pygen_proces"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("pygen_proces"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[PygenProcessApply],
                                list(_PYGENPROCESS_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=PygenProcess,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
