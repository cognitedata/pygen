from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import QueryStep, QueryAPI, T_DomainModelList
from markets.client.data_classes import (
    Market,
    MarketApply,
)
from markets.client.data_classes._market import (
    _MARKET_PROPERTIES_BY_FIELD,
)


class MarketQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_market: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_market: Whether to retrieve the market or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_market and not self._builder[-1].name.startswith("market"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("market"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[MarketApply],
                                list(_MARKET_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=Market,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
