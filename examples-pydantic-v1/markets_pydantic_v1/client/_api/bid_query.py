from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList
from markets_pydantic_v1.client.data_classes import (
    Bid,
    BidApply,
)
from markets_pydantic_v1.client.data_classes._bid import (
    _BID_PROPERTIES_BY_FIELD,
)


class BidQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_bid: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_bid: Whether to retrieve the bid or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_bid and not self._builder[-1].name.startswith("bid"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("bid"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[BidApply],
                                list(_BID_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=Bid,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
