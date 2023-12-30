from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from omni_pydantic_v1.data_classes import (
    DomainModelApply,
    ConnectionItemC,
    ConnectionItemCApply,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .connection_item_a_query import ConnectionItemAQueryAPI
    from .connection_item_b_query import ConnectionItemBQueryAPI


class ConnectionItemCQueryAPI(QueryAPI[T_DomainModelList]):
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
                name=self._builder.next_name("connection_item_c"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select(
                    [dm.query.SourceSelector(self._view_by_write_class[ConnectionItemCApply], ["*"])]
                ),
                result_cls=ConnectionItemC,
                max_retrieve_limit=limit,
            )
        )

    def connection_item_a(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
    ) -> ConnectionItemAQueryAPI[T_DomainModelList]:
        """Query along the connection item a edges of the connection item c.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item a edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            ConnectionItemAQueryAPI: The query API for the connection item a.
        """
        from .connection_item_a_query import ConnectionItemAQueryAPI

        from_ = self._builder[-1].name

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("pygen-models", "unidirectional"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("connection_item_a"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        return ConnectionItemAQueryAPI(self._client, self._builder, self._view_by_write_class, None, limit)

    def connection_item_b(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
    ) -> ConnectionItemBQueryAPI[T_DomainModelList]:
        """Query along the connection item b edges of the connection item c.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of connection item b edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            ConnectionItemBQueryAPI: The query API for the connection item b.
        """
        from .connection_item_b_query import ConnectionItemBQueryAPI

        from_ = self._builder[-1].name

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("pygen-models", "unidirectional"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("connection_item_b"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        return ConnectionItemBQueryAPI(self._client, self._builder, self._view_by_write_class, None, limit)

    def query(
        self,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Returns:
            The list of the source nodes of the query.

        """
        return self._query()
