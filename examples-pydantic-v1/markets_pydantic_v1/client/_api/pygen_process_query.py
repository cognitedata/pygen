from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from markets_pydantic_v1.client.data_classes import (
    DomainModelApply,
    PygenProcess,
    PygenProcessApply,
    Bid,
    BidApply,
    DateTransformationPair,
    DateTransformationPairApply,
    ValueTransformation,
    ValueTransformationApply,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter


class PygenProcessQueryAPI(QueryAPI[T_DomainModelList]):
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
                name=self._builder.next_name("pygen_proces"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_by_write_class[PygenProcessApply], ["*"])]),
                result_cls=PygenProcess,
                max_retrieve_limit=limit,
            )
        )

    def query(
        self,
        retrieve_bid: bool = False,
        retrieve_date_transformations: bool = False,
        retrieve_transformation: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_bid: Whether to retrieve the bid for each pygen proces or not.
            retrieve_date_transformations: Whether to retrieve the date transformation for each pygen proces or not.
            retrieve_transformation: Whether to retrieve the transformation for each pygen proces or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_bid:
            self._query_append_bid(from_)
        if retrieve_date_transformations:
            self._query_append_date_transformations(from_)
        if retrieve_transformation:
            self._query_append_transformation(from_)
        return self._query()

    def _query_append_bid(self, from_: str) -> None:
        view_id = self._view_by_write_class[BidApply]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("bid"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_write_class[PygenProcessApply].as_property_ref("bid"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=Bid,
            ),
        )

    def _query_append_date_transformations(self, from_: str) -> None:
        view_id = self._view_by_write_class[DateTransformationPairApply]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("date_transformations"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_write_class[PygenProcessApply].as_property_ref("date_transformations"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=DateTransformationPair,
            ),
        )

    def _query_append_transformation(self, from_: str) -> None:
        view_id = self._view_by_write_class[ValueTransformationApply]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("transformation"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_write_class[PygenProcessApply].as_property_ref("transformation"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=ValueTransformation,
            ),
        )
