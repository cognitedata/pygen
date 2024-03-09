from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from omni.data_classes import (
    DomainModelCore,
    ConnectionItemD,
    ConnectionItemE,
    ConnectionItemE,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter


class ConnectionItemDQueryAPI(QueryAPI[T_DomainModelList]):
    def __init__(
        self,
        client: CogniteClient,
        builder: QueryBuilder[T_DomainModelList],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId],
        filter_: dm.filters.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ):
        super().__init__(client, builder, view_by_read_class)

        self._builder.append(
            QueryStep(
                name=self._builder.next_name("connection_item_d"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_by_read_class[ConnectionItemD], ["*"])]),
                result_cls=ConnectionItemD,
                max_retrieve_limit=limit,
            )
        )

    def query(
        self,
        retrieve_direct_multi: bool = False,
        retrieve_direct_single: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_direct_multi: Whether to retrieve the direct multi for each connection item d or not.
            retrieve_direct_single: Whether to retrieve the direct single for each connection item d or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_direct_multi:
            self._query_append_direct_multi(from_)
        if retrieve_direct_single:
            self._query_append_direct_single(from_)
        return self._query()

    def _query_append_direct_multi(self, from_: str) -> None:
        view_id = self._view_by_read_class[ConnectionItemE]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("direct_multi"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[ConnectionItemD].as_property_ref("directMulti"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=ConnectionItemE,
            ),
        )

    def _query_append_direct_single(self, from_: str) -> None:
        view_id = self._view_by_read_class[ConnectionItemE]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("direct_single"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[ConnectionItemD].as_property_ref("directSingle"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=ConnectionItemE,
            ),
        )
