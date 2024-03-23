from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from omni.data_classes import (
    DomainModelCore,
    ConnectionItemA,
    ConnectionItemC,
    ConnectionItemA,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .connection_item_b_query import ConnectionItemBQueryAPI


class ConnectionItemAQueryAPI(QueryAPI[T_DomainModelList]):
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
                name=self._builder.next_name("connection_item_a"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_by_read_class[ConnectionItemA], ["*"])]),
                result_cls=ConnectionItemA,
                max_retrieve_limit=limit,
            )
        )

    def outwards(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_other_direct: bool = False,
        retrieve_self_direct: bool = False,
    ) -> ConnectionItemBQueryAPI[T_DomainModelList]:
        """Query along the outward edges of the connection item a.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of outward edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_other_direct: Whether to retrieve the other direct for each connection item a or not.
            retrieve_self_direct: Whether to retrieve the self direct for each connection item a or not.

        Returns:
            ConnectionItemBQueryAPI: The query API for the connection item b.
        """
        from .connection_item_b_query import ConnectionItemBQueryAPI

        from_ = self._builder[-1].name

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("pygen-models", "bidirectional"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("outwards"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        if retrieve_other_direct:
            self._query_append_other_direct(from_)
        if retrieve_self_direct:
            self._query_append_self_direct(from_)
        return ConnectionItemBQueryAPI(self._client, self._builder, self._view_by_read_class, None, limit)

    def query(
        self,
        retrieve_other_direct: bool = False,
        retrieve_self_direct: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_other_direct: Whether to retrieve the other direct for each connection item a or not.
            retrieve_self_direct: Whether to retrieve the self direct for each connection item a or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_other_direct:
            self._query_append_other_direct(from_)
        if retrieve_self_direct:
            self._query_append_self_direct(from_)
        return self._query()

    def _query_append_other_direct(self, from_: str) -> None:
        view_id = self._view_by_read_class[ConnectionItemC]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("other_direct"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[ConnectionItemA].as_property_ref("otherDirect"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=ConnectionItemC,
                is_single_direct_relation=True,
            ),
        )

    def _query_append_self_direct(self, from_: str) -> None:
        view_id = self._view_by_read_class[ConnectionItemA]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("self_direct"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[ConnectionItemA].as_property_ref("selfDirect"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=ConnectionItemA,
                is_single_direct_relation=True,
            ),
        )
