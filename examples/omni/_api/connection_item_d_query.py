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

if TYPE_CHECKING:
    from .connection_item_e_query import ConnectionItemEQueryAPI


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

    def outwards_single(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_direct_multi: bool = False,
        retrieve_direct_single: bool = False,
    ) -> ConnectionItemEQueryAPI[T_DomainModelList]:
        """Query along the outwards single edges of the connection item d.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of outwards single edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_direct_multi: Whether to retrieve the direct multi for each connection item d or not.
            retrieve_direct_single: Whether to retrieve the direct single for each connection item d or not.

        Returns:
            ConnectionItemEQueryAPI: The query API for the connection item e.
        """
        from .connection_item_e_query import ConnectionItemEQueryAPI

        from_ = self._builder[-1].name

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("pygen-models", "bidirectionalSingle"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("outwards_single"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        if retrieve_direct_multi:
            self._query_append_direct_multi(from_)
        if retrieve_direct_single:
            self._query_append_direct_single(from_)
        return ConnectionItemEQueryAPI(self._client, self._builder, self._view_by_read_class, None, limit)

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
