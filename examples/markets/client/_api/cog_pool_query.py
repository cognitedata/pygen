from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import QueryStep, QueryAPI, T_DomainModelList
from markets.client.data_classes import (
    CogPool,
    CogPoolApply,
)
from markets.client.data_classes._cog_pool import (
    _COGPOOL_PROPERTIES_BY_FIELD,
)


class CogPoolQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_cog_pool: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_cog_pool: Whether to retrieve the cog pool or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_cog_pool and not self._builder[-1].name.startswith("cog_pool"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("cog_pool"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[CogPoolApply],
                                list(_COGPOOL_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=CogPool,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()