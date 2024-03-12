from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from omni.data_classes import (
    DomainModelCore,
    ConnectionItemE,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .connection_item_d_query import ConnectionItemDQueryAPI


class ConnectionItemEQueryAPI(QueryAPI[T_DomainModelList]):
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
                name=self._builder.next_name("connection_item_e"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_by_read_class[ConnectionItemE], ["*"])]),
                result_cls=ConnectionItemE,
                max_retrieve_limit=limit,
            )
        )

    def inwards_single(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
    ) -> ConnectionItemDQueryAPI[T_DomainModelList]:
        """Query along the inwards single edges of the connection item e.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of inwards single edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            ConnectionItemDQueryAPI: The query API for the connection item d.
        """
        from .connection_item_d_query import ConnectionItemDQueryAPI

        from_ = self._builder[-1].name

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("pygen-models", "bidirectionalSingle"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("inwards_single"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="inwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        return ConnectionItemDQueryAPI(self._client, self._builder, self._view_by_read_class, None, limit)

    def query(
        self,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Returns:
            The list of the source nodes of the query.

        """
        return self._query()
