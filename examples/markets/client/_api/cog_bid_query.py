from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from markets.client.data_classes import (
    DomainModelApply,
    CogBid,
    CogBidApply,
    Market,
    MarketApply,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList


class CogBidQueryAPI(QueryAPI[T_DomainModelList]):
    def __init__(
        self,
        client: CogniteClient,
        builder: QueryBuilder[T_DomainModelList],
        view_by_write_class: dict[type[DomainModelApply], dm.ViewId],
        filter_: dm.filters.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ):
        super().__init__(client, builder, view_by_write_class)

        self._builder.append(
            QueryStep(
                name=self._builder.next_name("cog_bid"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_by_write_class[CogBidApply], ["*"])]),
                result_cls=CogBid,
                max_retrieve_limit=limit,
            )
        )

    def query(
        self,
        retrieve_market: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_market: Whether to retrieve the market for each cog bid or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_market:
            self._query_append_market(from_)
        return self._query()

    def _query_append_person(self, from_: str) -> None:
        view_id = self._view_by_write_class[MarketApply]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("market"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_write_class[CogBidApply].as_property_ref("person"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=Market,
            ),
        )
